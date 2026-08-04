package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

func TestNewDeclaresClaudeBackend(t *testing.T) {
	if adapter := New(); adapter == nil || adapter.Backend() != Backend {
		t.Fatalf("adapter = %#v", adapter)
	}
}

func TestForkPrompt(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "fork.trace")
	adapter := New(
		acpx.WithCommand(os.Args[0]),
		acpx.WithArgs("-test.run=TestClaudeForkChild", "--"),
		acpx.WithEnvironment(append(os.Environ(), "DURABLE_CLAUDE_FORK_CHILD=1", "DURABLE_CLAUDE_FORK_TRACE="+trace)),
	)
	if _, err := adapter.StartSession(context.Background(), "host", host.StartSessionRequest{Worktree: t.TempDir()}, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("host") }()
	response, err := adapter.ForkPrompt(context.Background(), host.ForkPromptRequest{
		SessionID: "host", Prompt: "review", Instructions: "be precise",
		MCPServers: []host.ForkMCPServer{{
			Type: "stdio", Name: "review", Command: "review", Args: []string{"--json"},
			Env: []host.ForkMCPEnv{{Name: "A", Value: "B"}}, Headers: []host.ForkMCPHTTPHeader{{Name: "X-Test", Value: "yes"}},
		}},
	})
	if err != nil || !response.Accepted {
		t.Fatalf("fork response = %#v, %v", response, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		raw, readErr := os.ReadFile(trace) //nolint:gosec // The test owns the isolated trace path.
		if readErr == nil && strings.Contains(string(raw), "prompt:provider-child") && strings.Contains(string(raw), "close:provider-child") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fork trace = %q, %v", raw, readErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, request := range []host.ForkPromptRequest{{Prompt: "prompt"}, {SessionID: "host"}, {SessionID: "missing", Prompt: "prompt"}} {
		if _, err := adapter.ForkPrompt(context.Background(), request); err == nil {
			t.Fatalf("invalid fork request succeeded: %#v", request)
		}
	}
}

func TestInterruptPreservesProcessStateBeforeNextTurn(t *testing.T) {
	adapter := New(
		acpx.WithCommand(os.Args[0]),
		acpx.WithArgs("-test.run=TestClaudeForkChild", "--"),
		acpx.WithEnvironment(append(os.Environ(), "DURABLE_CLAUDE_FORK_CHILD=1")),
	)
	events := make(chan host.Event, 32)
	if _, err := adapter.StartSession(context.Background(), "interrupt", host.StartSessionRequest{Worktree: t.TempDir()}, func(event host.Event) { events <- event }); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("interrupt") }()
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "start-background"}, nil); err != nil {
		t.Fatal(err)
	}
	waitClaudeEvent(t, events, func(event host.Event) bool { return event.Message == "claude background started" })
	waitClaudeEvent(t, events, func(event host.Event) bool { return event.Type == host.EventTurnComplete })
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "hang-until-cancel"}, nil); err != nil {
		t.Fatal(err)
	}
	waitClaudeEvent(t, events, func(event host.Event) bool { return event.Message == "claude waiting for cancel" })
	if err := adapter.Interrupt(context.Background(), "interrupt", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.SendTurn(context.Background(), "interrupt", host.SendTurnRequest{Prompt: "check-background"}, nil); err != nil {
		t.Fatal(err)
	}
	waitClaudeEvent(t, events, func(event host.Event) bool {
		if event.Type == host.EventAgentRecovered {
			t.Fatal("interrupt restarted the Claude process")
		}
		return event.Message == "claude background running"
	})
}

func waitClaudeEvent(t *testing.T, events <-chan host.Event, match func(host.Event) bool) {
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
			t.Fatal("timed out waiting for Claude event")
		}
	}
}

func TestClaudeForkChild(t *testing.T) {
	if os.Getenv("DURABLE_CLAUDE_FORK_CHILD") != "1" {
		return
	}
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	background := false
	for {
		var message claudeRPCMessage
		if err := decoder.Decode(&message); err != nil {
			return
		}
		switch message.Method {
		case "initialize":
			writeClaudeRPC(t, encoder, message.ID, map[string]any{"protocolVersion": 1})
		case "session/new":
			writeClaudeRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider-parent"})
		case "session/fork":
			writeClaudeRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider-child"})
		case "session/prompt":
			prompt := claudePromptText(message.Params)
			switch prompt {
			case "start-background":
				background = true
				writeClaudeUpdate(t, encoder, "agent_message_chunk", "claude background started")
			case "hang-until-cancel":
				writeClaudeUpdate(t, encoder, "agent_thought_chunk", "claude waiting for cancel")
				for {
					var next claudeRPCMessage
					if err := decoder.Decode(&next); err != nil {
						return
					}
					if next.Method == "session/cancel" {
						break
					}
				}
			case "check-background":
				text := "claude background missing"
				if background {
					text = "claude background running"
				}
				writeClaudeUpdate(t, encoder, "agent_message_chunk", text)
			default:
				appendClaudeTrace(t, "prompt:"+claudeSessionID(message.Params))
			}
			writeClaudeRPC(t, encoder, message.ID, map[string]any{"stopReason": "end_turn"})
		case "session/close":
			appendClaudeTrace(t, "close:"+claudeSessionID(message.Params))
			writeClaudeRPC(t, encoder, message.ID, map[string]any{})
		}
	}
}

func claudePromptText(raw json.RawMessage) string {
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

func writeClaudeUpdate(t *testing.T, encoder *json.Encoder, update, text string) {
	t.Helper()
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
		"sessionId": "provider-parent", "update": map[string]any{"sessionUpdate": update, "content": map[string]any{"type": "text", "text": text}},
	}}); err != nil {
		t.Fatal(err)
	}
}

type claudeRPCMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func writeClaudeRPC(t *testing.T, encoder *json.Encoder, id json.RawMessage, result any) {
	t.Helper()
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result}); err != nil {
		t.Fatal(err)
	}
}

func claudeSessionID(raw json.RawMessage) string {
	var params map[string]any
	_ = json.Unmarshal(raw, &params)
	value, _ := params["sessionId"].(string)
	return value
}

func appendClaudeTrace(t *testing.T, line string) {
	t.Helper()
	path := os.Getenv("DURABLE_CLAUDE_FORK_TRACE")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // The parent test owns the isolated trace path.
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteString(line + "\n"); err != nil {
		t.Fatal(err)
	}
}
