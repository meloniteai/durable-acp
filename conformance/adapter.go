package conformance

import (
	"context"
	"strings"
	"testing"

	"github.com/meloniteai/durable-acp/host"
)

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
