package host

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

type adapterStub struct{}

func (adapterStub) Backend() Backend {
	return Backend("test")
}

func (adapterStub) Detect(context.Context) BackendStatus {
	return BackendStatus{Backend: Backend("test"), Available: true}
}

func (adapterStub) StartSession(context.Context, string, StartSessionRequest, EventSink) (BackendSession, error) {
	return BackendSession{}, nil
}

func (adapterStub) SendTurn(context.Context, string, SendTurnRequest, EventSink) (BackendSession, error) {
	return BackendSession{}, nil
}

func (adapterStub) Interrupt(context.Context, string, EventSink) error {
	return nil
}

func (adapterStub) CloseSession(string) error {
	return nil
}

type forkingAdapter struct {
	adapterStub
}

func (forkingAdapter) ForkPrompt(context.Context, ForkPromptRequest) (ForkPromptResponse, error) {
	return ForkPromptResponse{Accepted: true}, nil
}

type capabilityAdapter struct {
	forkingAdapter
	supported bool
	reason    string
	seen      *string
}

func (a capabilityAdapter) SessionForkSupport(sessionID string) (bool, string) {
	*a.seen = sessionID
	return a.supported, a.reason
}

func TestAdapterSessionForkSupport(t *testing.T) {
	t.Parallel()

	if supported, reason := AdapterSessionForkSupport(adapterStub{}, "session-1"); supported || reason != "" {
		t.Fatalf("adapter without forker = (%t, %q), want (false, empty)", supported, reason)
	}
	if supported, reason := AdapterSessionForkSupport(forkingAdapter{}, "session-1"); !supported || reason != "" {
		t.Fatalf("forker = (%t, %q), want (true, empty)", supported, reason)
	}

	seen := ""
	adapter := capabilityAdapter{supported: false, reason: "disabled by provider", seen: &seen}
	if supported, reason := AdapterSessionForkSupport(adapter, "session-2"); supported || reason != adapter.reason {
		t.Fatalf("capability = (%t, %q), want (false, %q)", supported, reason, adapter.reason)
	}
	if seen != "session-2" {
		t.Fatalf("capability session = %q, want session-2", seen)
	}
}

func TestEventLocalMetadataIsNotSerialized(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(Event{
		SessionID:   "session-1",
		Type:        EventToolStarted,
		Local:       map[string]any{"replay": true, "replay_start": true},
		ToolDisplay: &ToolDisplay{Title: "Shell", Kind: "execute"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("local")) || bytes.Contains(raw, []byte("replay")) {
		t.Fatalf("event JSON exposes process-local metadata: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"tool_display":{"title":"Shell","kind":"execute"}`)) {
		t.Fatalf("event JSON is missing native tool display: %s", raw)
	}
}
