package journal

import (
	"encoding/json"
	"testing"

	"github.com/meloniteai/durable-acp/host"
)

func TestTranslate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event host.Event
		want  string
		ok    bool
	}{
		{name: "user message", event: host.Event{SessionID: "one", Type: host.EventMessage, Role: "user", Message: "hello"}, want: EventUserMessage, ok: true},
		{name: "agent message", event: host.Event{SessionID: "one", Type: host.EventMessage, Role: "assistant"}, want: EventAgentMessage, ok: true},
		{name: "permission", event: host.Event{Type: host.EventPermission}, want: EventAgentPermission, ok: true},
		{name: "workspace", event: host.Event{Type: host.EventFileChanged}, want: EventAgentWorkspace, ok: true},
		{name: "turn started", event: host.Event{Type: host.EventTurnStarted}, want: EventAgentTurnStarted, ok: true},
		{name: "yielded", event: host.Event{Type: host.EventTurnComplete}, want: EventAgentYielded, ok: true},
		{name: "failed", event: host.Event{Type: host.EventTurnFailed}, want: EventAgentTurnFailed, ok: true},
		{name: "process exited", event: host.Event{Type: host.EventProcessExited}, want: EventAgentProcessExited, ok: true},
		{name: "stalled", event: host.Event{Type: host.EventAgentStalled}, want: EventAgentStalled, ok: true},
		{name: "recovered", event: host.Event{Type: host.EventAgentRecovered}, want: EventAgentRecovered, ok: true},
		{name: "resume failed", event: host.Event{Type: host.EventAgentResumeFailed}, want: EventAgentResumeFailed, ok: true},
		{name: "plan", event: host.Event{Type: host.EventPlanUpdate}, want: EventAgentPlanProposed, ok: true},
		{name: "todo", event: host.Event{Type: host.EventTodoUpdate}, want: EventAgentTodoUpdated, ok: true},
		{name: "extension", event: host.Event{Type: host.EventType("example.state_changed")}, want: "example.state_changed", ok: true},
		{name: "thinking is ephemeral", event: host.Event{Type: host.EventThinking}, ok: false},
		{name: "tool start is ephemeral", event: host.Event{Type: host.EventToolStarted}, ok: false},
		{name: "tool output is ephemeral", event: host.Event{Type: host.EventToolOutput}, ok: false},
		{name: "trace is ephemeral", event: host.Event{Type: host.EventTraceUpdated}, ok: false},
		{name: "message without role", event: host.Event{Type: host.EventMessage}, ok: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, ok := Translate(test.event)
			if ok != test.ok || record.Event != test.want {
				t.Fatalf("Translate(%#v) = (%#v, %t), want event %q ok %t", test.event, record, ok, test.want, test.ok)
			}
		})
	}
}

func TestTranslatePreservesNeutralData(t *testing.T) {
	t.Parallel()

	record, ok := Translate(host.Event{
		SourceEventID:    "source-1",
		SessionID:        "one",
		Backend:          "codex",
		BackendSessionID: "backend-session",
		BackendThreadID:  "thread",
		BackendTurnID:    "turn",
		Type:             host.EventMessage,
		Role:             "assistant",
		Message:          "done",
		Data:             map[string]any{"source": "test"},
	})
	if !ok || record.SourceEventID != "source-1" || record.SessionID != "one" || record.TurnID != "turn" {
		t.Fatalf("record = %#v ok=%t", record, ok)
	}
	var data map[string]any
	if err := json.Unmarshal(record.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["message"] != "done" || data["source"] != "test" {
		t.Fatalf("data = %#v", data)
	}
	agent, ok := data["agent"].(map[string]any)
	if !ok || agent["backend"] != "codex" || agent["backend_turn_id"] != "turn" {
		t.Fatalf("agent data = %#v", data["agent"])
	}

	if _, ok := Translate(host.Event{Type: host.EventType("example.event"), Data: map[string]any{"bad": func() {}}}); ok {
		t.Fatal("Translate accepted data that cannot be encoded as JSON")
	}
}
