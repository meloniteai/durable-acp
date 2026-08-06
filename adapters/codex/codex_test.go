package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

func TestCodexResumeIdentityParsers(t *testing.T) {
	if count := codexResumeTurnCount(json.RawMessage(`{"thread":{"turns":[{},{}]}}`)); count != 2 {
		t.Fatalf("resume turn count = %d, want 2", count)
	}
	if count := codexResumeTurnCount(json.RawMessage(`{`)); count != 0 {
		t.Fatalf("invalid resume turn count = %d, want 0", count)
	}
	tests := []struct {
		raw  string
		want string
	}{
		{`{"method":"session/update","params":{"sessionId":"thread","update":{"sessionUpdate":"user_message_chunk","messageId":"item-1"}}}`, "thread:item-1"},
		{`{"method":"session/update","params":{"sessionId":"thread","update":{"sessionUpdate":"user_message_chunk","itemId":"item-2"}}}`, "thread:item-2"},
		{`{"method":"session/update","params":{"sessionId":"thread","update":{"sessionUpdate":"agent_message_chunk","messageId":"item-3"}}}`, ""},
		{`{"method":"session/update","params":{"sessionId":"thread","update":{"sessionUpdate":"user_message_chunk"}}}`, ""},
		{`{`, ""},
	}
	for _, test := range tests {
		if got := codexReplayTurnIdentity(json.RawMessage(test.raw)); got != test.want {
			t.Fatalf("replay turn identity = %q, want %q", got, test.want)
		}
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

func TestInterruptPreservesProcessBeforeNextTurn(t *testing.T) {
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
	sawMessage := false
	sawComplete := false
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for !sawMessage || !sawComplete {
		select {
		case event := <-events:
			if event.Type == host.EventAgentRecovered {
				t.Fatal("interrupt restarted the Codex process")
			}
			sawMessage = sawMessage || event.Message == "codex saw after-cancel"
			sawComplete = sawComplete || event.Type == host.EventTurnComplete
		case <-timer.C:
			t.Fatalf("follow-up did not complete on the existing process: message=%v complete=%v", sawMessage, sawComplete)
		}
	}
}

func TestResumeSeedsTurnSequenceFromCodexHistory(t *testing.T) {
	adapter := New(
		acpx.WithCommand(os.Args[0]),
		acpx.WithArgs("-test.run=TestCodexForkChild", "--"),
		acpx.WithEnvironment(append(os.Environ(), "DURABLE_CODEX_FORK_CHILD=1")),
	)
	if _, err := adapter.StartSession(context.Background(), "resume", host.StartSessionRequest{
		Worktree:               t.TempDir(),
		ResumeBackendSessionID: "provider-history",
	}, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("resume") }()
	turn, err := adapter.SendTurn(context.Background(), "resume", host.SendTurnRequest{Prompt: "after-history"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if turn.TurnID != "provider-history:3" {
		t.Fatalf("resumed turn id = %q, want provider-history:3", turn.TurnID)
	}
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
			var params struct {
				SessionID string `json:"sessionId"`
			}
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			if params.SessionID == "provider-history" {
				for index := 1; index <= 2; index++ {
					if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": params.SessionID, "update": map[string]any{"sessionUpdate": "user_message_chunk", "messageId": fmt.Sprintf("item-%d", index), "content": map[string]any{"type": "text", "text": "same"}}}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			writeCodexRPC(t, encoder, message.ID, map[string]any{"sessionId": params.SessionID})
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
