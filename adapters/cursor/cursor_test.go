package cursor

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

func TestNewDeclaresCursorBackend(t *testing.T) {
	if adapter := New(); adapter == nil || adapter.Backend() != Backend {
		t.Fatalf("adapter = %#v", adapter)
	}
	if defaultCommand != "cursor-agent" {
		t.Fatalf("default command = %q", defaultCommand)
	}
}

func TestDoneBeforeLatePermissionKeepsTurnActive(t *testing.T) {
	adapter := New(
		acpx.WithCommand(os.Args[0]),
		acpx.WithArgs("-test.run=TestLatePermissionChild", "--"),
		acpx.WithEnvironment(append(os.Environ(), "DURABLE_ACP_CURSOR_LATE_PERMISSION_CHILD=1")),
	)
	events := make(chan host.Event, 16)
	if _, err := adapter.StartSession(context.Background(), "host", host.StartSessionRequest{Worktree: t.TempDir()}, func(event host.Event) { events <- event }); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("host") }()
	if _, err := adapter.SendTurn(context.Background(), "host", host.SendTurnRequest{Prompt: "run"}, nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	requestID := ""
	for requestID == "" {
		select {
		case event := <-events:
			if event.Type == host.EventTurnComplete {
				t.Fatal("turn completed before the late permission request")
			}
			if event.Type == host.EventInteractionRequested && event.Interaction != nil {
				requestID = event.Interaction.ID
			}
		case <-deadline.C:
			t.Fatal("late permission request was not emitted")
		}
	}
	if err := adapter.RespondInteraction(context.Background(), "host", host.InteractionResponse{RequestID: requestID, Action: "approve"}); err != nil {
		t.Fatal(err)
	}

	message := false
	completed := false
	for !message || !completed {
		select {
		case event := <-events:
			message = message || event.Type == host.EventMessage && event.Message == "approved"
			completed = completed || event.Type == host.EventTurnComplete
		case <-deadline.C:
			t.Fatalf("turn did not complete after permission response: message=%v completed=%v", message, completed)
		}
	}
}

func TestInterruptPreservesProcessStateBeforeNextTurn(t *testing.T) {
	adapter := New(
		acpx.WithCommand(os.Args[0]),
		acpx.WithArgs("-test.run=TestLatePermissionChild", "--"),
		acpx.WithEnvironment(append(os.Environ(), "DURABLE_ACP_CURSOR_LATE_PERMISSION_CHILD=1")),
	)
	events := make(chan host.Event, 32)
	if _, err := adapter.StartSession(context.Background(), "interrupt", host.StartSessionRequest{Worktree: t.TempDir()}, func(event host.Event) { events <- event }); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("interrupt") }()
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "start-background"}, nil); err != nil {
		t.Fatal(err)
	}
	waitCursorEvent(t, events, func(event host.Event) bool { return event.Message == "cursor background started" })
	waitCursorEvent(t, events, func(event host.Event) bool { return event.Type == host.EventTurnComplete })
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "hang-until-cancel"}, nil); err != nil {
		t.Fatal(err)
	}
	waitCursorEvent(t, events, func(event host.Event) bool { return event.Message == "cursor waiting for cancel" })
	if err := adapter.Interrupt(context.Background(), "interrupt", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "check-background"}, nil); err != nil {
		t.Fatal(err)
	}
	waitCursorEvent(t, events, func(event host.Event) bool {
		if event.Type == host.EventAgentRecovered {
			t.Fatal("interrupt restarted the Cursor process")
		}
		return event.Message == "cursor background running"
	})
}

func waitCursorEvent(t *testing.T, events <-chan host.Event, match func(host.Event) bool) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if match(event) {
				return
			}
		case <-timer.C:
			t.Fatal("timed out waiting for Cursor event")
		}
	}
}

func TestLatePermissionChild(t *testing.T) {
	if os.Getenv("DURABLE_ACP_CURSOR_LATE_PERMISSION_CHILD") != "1" {
		return
	}
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	background := false
	for {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := decoder.Decode(&request); err != nil {
			return
		}
		switch request.Method {
		case "initialize":
			writeCursorTestResponse(t, encoder, request.ID, map[string]any{"protocolVersion": 1})
		case "session/new":
			writeCursorTestResponse(t, encoder, request.ID, map[string]any{"sessionId": "cursor-session-1"})
		case "session/prompt":
			prompt := cursorPromptText(request.Params)
			switch prompt {
			case "start-background":
				background = true
				writeCursorUpdate(t, encoder, "agent_message_chunk", "cursor background started")
				writeCursorTestResponse(t, encoder, request.ID, map[string]any{"stopReason": "end_turn"})
				continue
			case "hang-until-cancel":
				writeCursorUpdate(t, encoder, "agent_thought_chunk", "cursor waiting for cancel")
				for {
					var next struct {
						Method string `json:"method"`
					}
					if err := decoder.Decode(&next); err != nil {
						return
					}
					if next.Method == "session/cancel" {
						break
					}
				}
				writeCursorTestResponse(t, encoder, request.ID, map[string]any{"stopReason": "end_turn"})
				continue
			case "check-background":
				text := "cursor background missing"
				if background {
					text = "cursor background running"
				}
				writeCursorUpdate(t, encoder, "agent_message_chunk", text)
				writeCursorTestResponse(t, encoder, request.ID, map[string]any{"stopReason": "end_turn"})
				continue
			}
			if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "cursor-session-1", "update": map[string]any{"sessionUpdate": "done"},
			}}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(150 * time.Millisecond)
			if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 43, "method": "session/request_permission", "params": map[string]any{
				"sessionId": "cursor-session-1",
				"toolCall":  map[string]any{"toolCallId": "tool-1", "title": "Shell", "kind": "execute"},
				"options": []map[string]any{
					{"optionId": "allow", "name": "Allow", "kind": "allow_once"},
					{"optionId": "deny", "name": "Deny", "kind": "reject_once"},
				},
			}}); err != nil {
				t.Fatal(err)
			}
			var response json.RawMessage
			if err := decoder.Decode(&response); err != nil {
				t.Fatal(err)
			}
			if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
				"sessionId": "cursor-session-1", "update": map[string]any{
					"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "approved"},
				},
			}}); err != nil {
				t.Fatal(err)
			}
			writeCursorTestResponse(t, encoder, request.ID, map[string]any{"stopReason": "end_turn"})
		}
	}
}

func cursorPromptText(raw json.RawMessage) string {
	var params struct {
		Prompt []struct {
			Text string `json:"text"`
		} `json:"prompt"`
	}
	_ = json.Unmarshal(raw, &params)
	if len(params.Prompt) == 0 {
		return ""
	}
	return params.Prompt[0].Text
}

func writeCursorUpdate(t *testing.T, encoder *json.Encoder, update, text string) {
	t.Helper()
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
		"sessionId": "cursor-session-1", "update": map[string]any{"sessionUpdate": update, "content": map[string]any{"type": "text", "text": text}},
	}}); err != nil {
		t.Fatal(err)
	}
}

func writeCursorTestResponse(t *testing.T, encoder *json.Encoder, id json.RawMessage, result any) {
	t.Helper()
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result}); err != nil {
		t.Fatal(err)
	}
}
