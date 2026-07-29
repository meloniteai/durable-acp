package conformance

import (
	"context"
	"testing"

	"github.com/meloniteai/durable-acp/host"
)

type adapterFixture struct {
	started     bool
	turned      bool
	interrupted bool
	closed      bool
	interaction bool
	responded   chan struct{}
}

func (*adapterFixture) Backend() host.Backend { return "fixture" }

func (*adapterFixture) Detect(context.Context) host.BackendStatus {
	return host.BackendStatus{Backend: "fixture", Available: true}
}

func (a *adapterFixture) StartSession(
	_ context.Context,
	sessionID string,
	req host.StartSessionRequest,
	emit host.EventSink,
) (host.BackendSession, error) {
	a.started = sessionID != "" && req.Backend == "fixture"
	emit(host.Event{Type: host.EventTurnStarted})
	return host.BackendSession{ID: "backend-session"}, nil
}

func (a *adapterFixture) SendTurn(
	_ context.Context,
	sessionID string,
	req host.SendTurnRequest,
	emit host.EventSink,
) (host.BackendSession, error) {
	a.turned = sessionID != "" && req.SessionID == sessionID
	if a.interaction {
		emit(host.Event{Type: host.EventInteractionRequested, Interaction: &host.InteractionRequest{ID: "interaction", Kind: host.InteractionPermission}})
		<-a.responded
	}
	emit(host.Event{Type: host.EventTurnComplete})
	return host.BackendSession{ID: "backend-session", TurnID: "turn-1"}, nil
}

func (a *adapterFixture) Interrupt(context.Context, string, host.EventSink) error {
	a.interrupted = true
	return nil
}

func (a *adapterFixture) CloseSession(string) error {
	a.closed = true
	return nil
}

func (a *adapterFixture) RespondInteraction(_ context.Context, _ string, response host.InteractionResponse) error {
	if response.RequestID != "interaction" {
		return context.Canceled
	}
	close(a.responded)
	return nil
}

func TestCertifyAdapter(t *testing.T) {
	t.Parallel()

	fixture := &adapterFixture{}
	events := 0
	CertifyAdapter(t, context.Background(), AdapterTest{
		Adapter:   fixture,
		SessionID: "session-1",
		Start:     host.StartSessionRequest{Worktree: "/repo"},
		Turn:      &host.SendTurnRequest{Prompt: "test"},
		Interrupt: true,
		Emit:      func(host.Event) { events++ },
	})
	if !fixture.started || !fixture.turned || !fixture.interrupted || !fixture.closed {
		t.Fatalf("incomplete lifecycle: %+v", fixture)
	}
	if events != 2 {
		t.Fatalf("events = %d, want 2", events)
	}
}

func TestRunAdapterAndLiveFlag(t *testing.T) {
	fixture := &adapterFixture{}
	RunAdapter(t, fixture, AdapterConfig{Worktree: t.TempDir(), SessionID: "conformance-session"})
	if !fixture.started || !fixture.turned || !fixture.interrupted || !fixture.closed {
		t.Fatalf("incomplete lifecycle: %+v", fixture)
	}
	t.Setenv("DURABLE_ACP_LIVE", "1")
	if !LiveAdapterEnabled() {
		t.Fatal("live adapter flag was not recognized")
	}
	t.Setenv("DURABLE_ACP_LIVE", "false")
	if LiveAdapterEnabled() {
		t.Fatal("false live adapter flag was recognized")
	}
}

func TestRunAdapterWithInteraction(t *testing.T) {
	fixture := &adapterFixture{interaction: true, responded: make(chan struct{})}
	RunAdapter(t, fixture, AdapterConfig{Worktree: t.TempDir(), RequireInteraction: true})
	if !fixture.turned || !fixture.closed {
		t.Fatalf("interaction lifecycle incomplete: %+v", fixture)
	}
}
