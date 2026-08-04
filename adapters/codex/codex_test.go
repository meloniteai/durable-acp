package codex

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

func TestNewDeclaresCodexBackend(t *testing.T) {
	if adapter := New(); adapter == nil || adapter.Backend() != Backend {
		t.Fatalf("adapter = %#v", adapter)
	}
}

func TestForkPrompt(t *testing.T) {
	adapter := New(
		acpx.WithCommand(os.Args[0]),
		acpx.WithArgs("-test.run=TestCodexForkChild", "--"),
		acpx.WithEnvironment(append(os.Environ(), "DURABLE_CODEX_FORK_CHILD=1")),
	)
	if _, err := adapter.StartSession(context.Background(), "host", host.StartSessionRequest{Worktree: t.TempDir()}, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("host") }()
	response, err := adapter.ForkPrompt(context.Background(), host.ForkPromptRequest{SessionID: "host", Prompt: "review", MCPServers: []host.ForkMCPServer{{Name: "review"}}})
	if err != nil || !response.Accepted {
		t.Fatalf("fork response = %#v, %v", response, err)
	}
	for _, request := range []host.ForkPromptRequest{{Prompt: "prompt"}, {SessionID: "host"}, {SessionID: "missing", Prompt: "prompt"}} {
		if _, err := adapter.ForkPrompt(context.Background(), request); err == nil {
			t.Fatalf("invalid fork request succeeded: %#v", request)
		}
	}
}

func TestInterruptReconnectsBeforeNextTurn(t *testing.T) {
	adapter := New(
		acpx.WithCommand(os.Args[0]),
		acpx.WithArgs("-test.run=TestCodexForkChild", "--"),
		acpx.WithEnvironment(append(os.Environ(), "DURABLE_CODEX_FORK_CHILD=1")),
	)
	events := make(chan host.Event, 32)
	if _, err := adapter.StartSession(context.Background(), "interrupt", host.StartSessionRequest{Worktree: t.TempDir()}, func(event host.Event) { events <- event }); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("interrupt") }()
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "hang-until-cancel"}, nil); err != nil {
		t.Fatal(err)
	}
	waitCodexEvent(t, events, func(event host.Event) bool { return event.Message == "codex waiting for cancel" })
	if err := adapter.Interrupt(context.Background(), "interrupt", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "after-cancel"}, nil); err != nil {
		t.Fatal(err)
	}
	waitCodexEvent(t, events, func(event host.Event) bool { return event.Type == host.EventAgentRecovered })
	waitCodexEvent(t, events, func(event host.Event) bool { return event.Message == "codex saw after-cancel" })
	waitCodexEvent(t, events, func(event host.Event) bool { return event.Type == host.EventTurnComplete })
}

func waitCodexEvent(t *testing.T, events <-chan host.Event, match func(host.Event) bool) {
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
			t.Fatal("timed out waiting for Codex event")
		}
	}
}

func TestCodexForkChild(t *testing.T) {
	if os.Getenv("DURABLE_CODEX_FORK_CHILD") != "1" {
		return
	}
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	for {
		var message codexRPCMessage
		if err := decoder.Decode(&message); err != nil {
			return
		}
		switch message.Method {
		case "initialize":
			var params map[string]any
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			capabilities, _ := params["clientCapabilities"].(map[string]any)
			if _, ok := capabilities["plan"].(map[string]any); !ok {
				t.Fatalf("client capabilities = %#v, want plan capability", capabilities)
			}
			if _, ok := params["capabilities"].(map[string]any); !ok {
				t.Fatalf("initialize params = %#v, want capabilities", params)
			}
			writeCodexRPC(t, encoder, message.ID, map[string]any{"protocolVersion": 1})
		case "session/new":
			writeCodexRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider"})
		case "session/resume", "session/load":
			writeCodexRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider"})
		case "session/prompt":
			var params struct {
				Prompt []struct {
					Text string `json:"text"`
				} `json:"prompt"`
			}
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			prompt := ""
			if len(params.Prompt) > 0 {
				prompt = params.Prompt[0].Text
			}
			update := "agent_message_chunk"
			text := "codex saw " + prompt
			if prompt == "hang-until-cancel" {
				update = "agent_thought_chunk"
				text = "codex waiting for cancel"
			}
			if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "provider", "update": map[string]any{"sessionUpdate": update, "content": map[string]any{"type": "text", "text": text}}}}); err != nil {
				t.Fatal(err)
			}
			if prompt != "hang-until-cancel" {
				writeCodexRPC(t, encoder, message.ID, map[string]any{"stopReason": "end_turn"})
			}
		case "codex/fork_prompt":
			writeCodexRPC(t, encoder, message.ID, map[string]any{"accepted": true})
		}
	}
}

type codexRPCMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func writeCodexRPC(t *testing.T, encoder *json.Encoder, id json.RawMessage, result any) {
	t.Helper()
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result}); err != nil {
		t.Fatal(err)
	}
}
