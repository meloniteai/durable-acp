package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"

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
