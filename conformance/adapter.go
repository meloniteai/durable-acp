package conformance

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/host"
)

// TestingT is the small testing surface used by RunAdapter. *testing.T
// satisfies it, and defining it here keeps the reusable contract independent
// from the Go testing package at runtime.
type TestingT interface {
	Helper()
	Fatalf(string, ...any)
	Cleanup(func())
}

// AdapterConfig supplies the runtime-dependent parts of an adapter contract
// test. Callers normally point Command configuration at a scripted ACP child;
// live smoke tests can instead select a real installed executable.
type AdapterConfig struct {
	Worktree           string
	SessionID          string
	Start              host.StartSessionRequest
	Turn               host.SendTurnRequest
	RequireInteraction bool
	Timeout            time.Duration
}

// AdapterTest describes one provider adapter lifecycle to certify.
type AdapterTest struct {
	Adapter   host.Adapter
	SessionID string
	Start     host.StartSessionRequest
	Turn      *host.SendTurnRequest
	Interrupt bool
	Emit      host.EventSink
}

// CertifyAdapter exercises the complete host.Adapter contract. Provider
// packages call it from their own tests with a hermetic adapter fixture.
func CertifyAdapter(t *testing.T, ctx context.Context, test AdapterTest) {
	t.Helper()
	if test.Adapter == nil {
		t.Fatal("conformance: adapter is required")
	}
	backend := test.Adapter.Backend()
	if strings.TrimSpace(string(backend)) == "" {
		t.Fatal("conformance: adapter backend is required")
	}
	status := test.Adapter.Detect(ctx)
	if status.Backend != backend {
		t.Fatalf("conformance: Detect backend = %q, want %q", status.Backend, backend)
	}
	if !status.Available {
		t.Fatalf("conformance: adapter unavailable: %s", status.Error)
	}

	sessionID := strings.TrimSpace(test.SessionID)
	if sessionID == "" {
		sessionID = "durable-acp-conformance"
	}
	start := test.Start
	if start.Backend == "" {
		start.Backend = backend
	}
	if start.Backend != backend {
		t.Fatalf("conformance: start backend = %q, want %q", start.Backend, backend)
	}
	emit := test.Emit
	if emit == nil {
		emit = func(host.Event) {}
	}

	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = test.Adapter.CloseSession(sessionID)
		}
	})
	if _, err := test.Adapter.StartSession(ctx, sessionID, start, emit); err != nil {
		t.Fatalf("conformance: StartSession: %v", err)
	}
	if test.Turn != nil {
		turn := *test.Turn
		if turn.SessionID == "" {
			turn.SessionID = sessionID
		}
		if turn.SessionID != sessionID {
			t.Fatalf("conformance: turn session = %q, want %q", turn.SessionID, sessionID)
		}
		if _, err := test.Adapter.SendTurn(ctx, sessionID, turn, emit); err != nil {
			t.Fatalf("conformance: SendTurn: %v", err)
		}
	}
	if test.Interrupt {
		if err := test.Adapter.Interrupt(ctx, sessionID, emit); err != nil {
			t.Fatalf("conformance: Interrupt: %v", err)
		}
	}
	if err := test.Adapter.CloseSession(sessionID); err != nil {
		t.Fatalf("conformance: CloseSession: %v", err)
	}
	closed = true
}

// RunAdapter verifies the portable host.Adapter contract: detection, session
// startup, normalized turn lifecycle, optional interaction round-trip,
// cancellation, and process close. It is intentionally provider-neutral so a
// third party can use it for their own adapter package.
func RunAdapter(t TestingT, adapter host.Adapter, config AdapterConfig) {
	t.Helper()
	if adapter == nil || strings.TrimSpace(string(adapter.Backend())) == "" {
		t.Fatalf("adapter must declare a backend")
	}
	if strings.TrimSpace(config.Worktree) == "" {
		t.Fatalf("adapter contract requires a worktree")
	}
	if strings.TrimSpace(config.SessionID) == "" {
		config.SessionID = "conformance-session"
	}
	if config.Timeout <= 0 {
		config.Timeout = 5 * time.Second
	}
	config.Start.SessionID = config.SessionID
	config.Start.Backend = adapter.Backend()
	config.Start.Worktree = config.Worktree
	config.Turn.SessionID = config.SessionID
	if strings.TrimSpace(config.Turn.Prompt) == "" && len(config.Turn.Attachments) == 0 {
		config.Turn.Prompt = "conformance prompt"
	}
	if !adapter.Detect(context.Background()).Available {
		t.Fatalf("adapter %q was not detected", adapter.Backend())
	}

	events := make(chan host.Event, 64)
	state, err := adapter.StartSession(context.Background(), config.SessionID, config.Start, func(event host.Event) { events <- event })
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	if state.ID == "" && state.ThreadID == "" {
		t.Fatalf("start session returned no backend identity")
	}
	t.Cleanup(func() { _ = adapter.CloseSession(config.SessionID) })

	turnResult := make(chan error, 1)
	go func() {
		_, sendErr := adapter.SendTurn(context.Background(), config.SessionID, config.Turn, func(event host.Event) { events <- event })
		turnResult <- sendErr
	}()

	deadline := time.NewTimer(config.Timeout)
	defer deadline.Stop()
	sawStart := false
	sawTerminal := false
	sawInteraction := false
	for !sawTerminal {
		select {
		case event := <-events:
			//exhaustive:ignore Provider-specific events intentionally need no generic assertion.
			switch event.Type {
			case host.EventTurnStarted:
				sawStart = true
			case host.EventInteractionRequested:
				sawInteraction = true
				if config.RequireInteraction {
					responder, ok := adapter.(host.InteractionResponder)
					if !ok || event.Interaction == nil {
						t.Fatalf("adapter emitted an interaction without responder support")
					}
					if respondErr := responder.RespondInteraction(context.Background(), config.SessionID, host.InteractionResponse{RequestID: event.Interaction.ID, Action: "approve"}); respondErr != nil {
						t.Fatalf("respond interaction: %v", respondErr)
					}
				}
			case host.EventTurnComplete, host.EventTurnFailed:
				sawTerminal = true
			default:
			}
		case sendErr := <-turnResult:
			if sendErr != nil {
				t.Fatalf("send turn: %v", sendErr)
			}
		case <-deadline.C:
			t.Fatalf("adapter %q did not complete a turn within %s", adapter.Backend(), config.Timeout)
		}
	}
	if !sawStart {
		t.Fatalf("adapter %q did not emit turn_started before terminal state", adapter.Backend())
	}
	if config.RequireInteraction && !sawInteraction {
		t.Fatalf("adapter %q did not emit the required interaction", adapter.Backend())
	}
	select {
	case sendErr := <-turnResult:
		if sendErr != nil {
			t.Fatalf("send turn: %v", sendErr)
		}
	default:
	}
	if err := adapter.Interrupt(context.Background(), config.SessionID, func(event host.Event) { events <- event }); err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	if err := adapter.CloseSession(config.SessionID); err != nil {
		t.Fatalf("close session: %v", err)
	}
}

// LiveAdapterEnabled reports whether a caller explicitly requested a live ACP
// smoke test. The contract runner itself never discovers or starts a provider
// unless the test chooses to do so.
func LiveAdapterEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("DURABLE_ACP_LIVE")), "1")
}
