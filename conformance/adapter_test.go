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
	a.started = sessionID == "session-1" && req.Backend == "fixture"
	emit(host.Event{Type: host.EventTurnStarted})
	return host.BackendSession{ID: "backend-session"}, nil
}

func (a *adapterFixture) SendTurn(
	_ context.Context,
	sessionID string,
	req host.SendTurnRequest,
	emit host.EventSink,
) (host.BackendSession, error) {
	a.turned = sessionID == "session-1" && req.SessionID == sessionID
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
