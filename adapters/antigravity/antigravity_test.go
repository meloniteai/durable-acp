package antigravity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/conformance"
	"github.com/meloniteai/durable-acp/host"

	antigravityacp "github.com/shubzkothekar/antigravity-acp-go"
)

func TestNewDeclaresAntigravityBackend(t *testing.T) {
	if adapter := New(); adapter == nil || adapter.Backend() != Backend {
		t.Fatalf("adapter = %#v", adapter)
	}
}

func TestAdapterConformance(t *testing.T) {
	provider := &fakeAgent{}
	adapter := New(Config{Command: os.Args[0], StateDir: t.TempDir()})
	adapter.newAgent = func(_, _, _, _, _, _ string) agent { return provider }
	conformance.RunAdapter(t, adapter, conformance.AdapterConfig{
		Worktree: t.TempDir(), SessionID: "host", Turn: host.SendTurnRequest{Prompt: "hello"}, RequireInteraction: true,
	})
	if !provider.closed {
		t.Fatal("provider session was not closed")
	}
}

func TestCatalogAndConfigTranslation(t *testing.T) {
	adapter := New(Config{Command: os.Args[0]})
	adapter.discoverModels = func(string) ([]string, error) { return []string{" model-a ", "", "model-b"}, nil }
	catalog, err := adapter.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Models) != 2 || catalog.Models[0].ID != "model-a" {
		t.Fatalf("catalog = %#v", catalog)
	}
	options := fakeConfigOptions("model-b", "auto")
	translated, model, mode := catalogFromOptions(options)
	if len(translated.Models) != 2 || len(translated.PermissionModes) != 2 || model != "model-b" || mode != "auto" {
		t.Fatalf("translated config = %#v model=%q mode=%q", translated, model, mode)
	}
}

func TestUpdateTranslation(t *testing.T) {
	events := make(chan host.Event, 32)
	managed := &managedSession{adapter: New(), backendID: "provider", emit: func(event host.Event) { events <- event }}
	client := &sessionClient{session: managed}
	updates := []*antigravityacp.SessionUpdate{
		{SessionUpdate: "agent_message_chunk", Content: map[string]any{"text": "hello"}},
		{SessionUpdate: "agent_thought_chunk", Content: "thinking"},
		{SessionUpdate: "tool_call", ToolCallID: "tool-1", Kind: "execute", RawInput: map[string]any{"command": "go test ./..."}},
		{SessionUpdate: "tool_call_update", ToolCallID: "tool-1", Kind: "edit", Status: "completed", Title: "Edit", RawInput: map[string]any{"path": "README.md"}, RawOutput: map[string]any{"ok": true}},
		{SessionUpdate: "available_commands_update", AvailableCommands: []antigravityacp.Command{{Name: "review", Description: "Review changes"}}},
		{SessionUpdate: "config_option_update", ConfigOptions: fakeConfigOptions("model-b", "auto")},
	}
	if err := client.Update("provider", nil); err != nil {
		t.Fatal(err)
	}
	for _, update := range updates {
		if err := client.Update("provider", update); err != nil {
			t.Fatal(err)
		}
	}
	want := []host.EventType{host.EventMessage, host.EventThinking, host.EventToolStarted, host.EventToolOutput, host.EventAvailableCommands, host.EventConfigCatalog}
	for _, eventType := range want {
		event := <-events
		if event.Type != eventType || event.BackendSessionID != "provider" || event.BackendThreadID != "provider" {
			t.Fatalf("event = %#v, want %q", event, eventType)
		}
	}
	if event := managed.translateUpdate(&antigravityacp.SessionUpdate{SessionUpdate: "unknown"}); event.Type != "" {
		t.Fatalf("unknown update = %#v", event)
	}
}

func TestRestartResponseAndValidation(t *testing.T) {
	provider := &fakeAgent{}
	events := make(chan host.Event, 32)
	adapter := New(Config{Command: os.Args[0], StateDir: t.TempDir()})
	adapter.newAgent = func(_, _, _, _, _, _ string) agent { return provider }
	worktree := t.TempDir()
	if _, err := adapter.StartSession(context.Background(), "host", host.StartSessionRequest{Worktree: worktree, Model: "model-b", PermissionMode: "auto"}, func(event host.Event) { events <- event }); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.StartSession(context.Background(), "host", host.StartSessionRequest{Worktree: worktree}, nil); err == nil {
		t.Fatal("duplicate session start succeeded")
	}
	state, err := adapter.RestartSession(context.Background(), "host", nil)
	if err != nil || state.ID != "provider" || provider.resumes != 1 {
		t.Fatalf("restart = %#v, %v, resumes=%d", state, err, provider.resumes)
	}
	managed, err := adapter.session("host")
	if err != nil {
		t.Fatal(err)
	}
	response := make(chan host.InteractionResponse, 1)
	managed.interactions["permission"] = response
	if err := adapter.RespondPermission(context.Background(), "host", "permission", true, "allow", ""); err != nil {
		t.Fatal(err)
	}
	if answer := <-response; answer.Action != "approve" || answer.Message != "allow" {
		t.Fatalf("permission response = %#v", answer)
	}
	if err := adapter.RespondInteraction(context.Background(), "host", host.InteractionResponse{RequestID: "missing"}); err == nil {
		t.Fatal("missing interaction response succeeded")
	}
	managed.turnMu.Lock()
	managed.turnID = "provider:active"
	turnCancelled := false
	managed.turnCancel = func() { turnCancelled = true }
	managed.turnMu.Unlock()
	if _, err := adapter.RestartSession(context.Background(), "host", nil); err == nil {
		t.Fatal("active restart succeeded")
	}
	if err := adapter.Interrupt(context.Background(), "host", nil); err != nil {
		t.Fatal(err)
	}
	if !turnCancelled || !provider.cancelled {
		t.Fatal("active interrupt did not cancel the turn")
	}
	if err := adapter.Interrupt(context.Background(), "host", nil); err != nil {
		t.Fatal(err)
	}
	if err := adapter.CloseSession("host"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.CloseSession("host"); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.RestartSession(context.Background(), "missing", nil); err == nil {
		t.Fatal("missing session restart succeeded")
	}
	if err := adapter.RespondInteraction(context.Background(), "missing", host.InteractionResponse{}); err == nil {
		t.Fatal("missing session interaction succeeded")
	}
	if _, err := adapter.StartSession(context.Background(), "", host.StartSessionRequest{Worktree: worktree}, nil); err == nil {
		t.Fatal("empty host session ID succeeded")
	}
	if _, err := adapter.StartSession(context.Background(), "relative", host.StartSessionRequest{Worktree: "relative"}, nil); err == nil {
		t.Fatal("relative worktree succeeded")
	}

	resumeFailure := New(Config{Command: os.Args[0], StateDir: t.TempDir()})
	resumeFailure.newAgent = func(_, _, _, _, _, _ string) agent { return &fakeAgent{resumeErr: errors.New("resume failed")} }
	if _, err := resumeFailure.StartSession(context.Background(), "resume", host.StartSessionRequest{Worktree: worktree, ResumeBackendSessionID: "provider"}, nil); err == nil {
		t.Fatal("failed resume succeeded")
	}
	emptyProvider := New(Config{Command: os.Args[0], StateDir: t.TempDir()})
	emptyProvider.newAgent = func(_, _, _, _, _, _ string) agent { return &fakeAgent{emptySessionID: true} }
	if _, err := emptyProvider.StartSession(context.Background(), "empty", host.StartSessionRequest{Worktree: worktree}, nil); err == nil {
		t.Fatal("empty provider session ID succeeded")
	}
}

func TestAdapterHelpersAndDiscoveryFailures(t *testing.T) {
	available := New(Config{Command: os.Args[0], ConversationsDir: t.TempDir(), StateDir: t.TempDir(), Version: "2"}, Config{Command: "ignored"})
	if status := available.Detect(context.Background()); !status.Available || status.Command == "" {
		t.Fatalf("available status = %#v", status)
	}
	conversations, state, err := available.directories()
	if err != nil || conversations == "" || state == "" {
		t.Fatalf("directories = %q, %q, %v", conversations, state, err)
	}
	unavailable := New(Config{Command: filepath.Join(t.TempDir(), "missing")})
	if status := unavailable.Detect(context.Background()); status.Available || status.Error == "" {
		t.Fatalf("unavailable status = %#v", status)
	}
	catalogFailure := New(Config{Command: os.Args[0]})
	catalogFailure.discoverModels = func(string) ([]string, error) { return nil, errors.New("discovery failed") }
	if _, err := catalogFailure.Catalog(context.Background()); err == nil {
		t.Fatal("catalog discovery failure succeeded")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateWorktree(file); err == nil {
		t.Fatal("file accepted as a worktree")
	}
	if valueMap(nil) != nil || valueMap("text") != nil || valueMap(func() {}) != nil {
		t.Fatal("invalid values converted to maps")
	}
	mapped := map[string]any{"nested": map[string]any{"value": true}}
	if valueMap(mapped)["nested"] == nil || mapValue(mapped, "nested")["value"] != true || mapValue(mapped, "missing") != nil {
		t.Fatalf("map helpers failed: %#v", mapped)
	}
	if contentText(nil) != "" || contentText(map[string]any{"text": " value "}) != "value" || outputText(nil) != "" || outputText("raw") != "raw" {
		t.Fatal("content helpers failed")
	}
}

func TestPromptFailureOutcomes(t *testing.T) {
	for _, test := range []struct {
		name     string
		provider *fakeAgent
		message  string
	}{
		{name: "error", provider: &fakeAgent{skipInteraction: true, promptErr: errors.New("prompt failed")}, message: "prompt failed"},
		{name: "outcome", provider: &fakeAgent{skipInteraction: true, outcome: &antigravityacp.PromptOutcome{Error: "provider failed"}}, message: "provider failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := make(chan host.Event, 8)
			adapter := New(Config{Command: os.Args[0], StateDir: t.TempDir()})
			adapter.newAgent = func(_, _, _, _, _, _ string) agent { return test.provider }
			if _, err := adapter.StartSession(context.Background(), "host", host.StartSessionRequest{Worktree: t.TempDir(), Prompt: "run"}, func(event host.Event) { events <- event }); err != nil {
				t.Fatal(err)
			}
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for {
				select {
				case event := <-events:
					if event.Type == host.EventTurnFailed {
						if event.Message != test.message {
							t.Fatalf("turn failure = %#v", event)
						}
						_ = adapter.CloseSession("host")
						return
					}
				case <-deadline.C:
					t.Fatal("timed out waiting for turn failure")
				}
			}
		})
	}
}

type fakeAgent struct {
	closed          bool
	resumes         int
	resumeErr       error
	emptySessionID  bool
	skipInteraction bool
	promptErr       error
	outcome         *antigravityacp.PromptOutcome
	cancelled       bool
}

func (a *fakeAgent) NewSession(_ string, _ []string, _ antigravityacp.Client) (string, []antigravityacp.ConfigOption) {
	if a.emptySessionID {
		return "", nil
	}
	return "provider", fakeConfigOptions("model-a", "ask")
}

func (a *fakeAgent) ResumeSession(_, _ string, _ []string, _ antigravityacp.Client) ([]antigravityacp.ConfigOption, error) {
	a.resumes++
	if a.resumeErr != nil {
		return nil, a.resumeErr
	}
	return fakeConfigOptions("model-a", "ask"), nil
}

func (a *fakeAgent) Prompt(_ string, _ any, client antigravityacp.Client) (*antigravityacp.PromptOutcome, error) {
	if a.promptErr != nil {
		return nil, a.promptErr
	}
	if a.outcome != nil {
		return a.outcome, nil
	}
	if err := client.Update("provider", &antigravityacp.SessionUpdate{SessionUpdate: "agent_message_chunk", Content: map[string]any{"text": "hello"}}); err != nil {
		return nil, err
	}
	if a.skipInteraction {
		return &antigravityacp.PromptOutcome{StopReason: "end_turn"}, nil
	}
	allowed, err := client.RequestPermission(map[string]any{"toolCall": map[string]any{"title": "Shell", "kind": "execute", "rawInput": map[string]any{"command": "true"}}})
	if err != nil {
		return nil, err
	}
	if allowed != true {
		return &antigravityacp.PromptOutcome{Error: "permission denied"}, nil
	}
	return &antigravityacp.PromptOutcome{StopReason: "end_turn"}, nil
}

func (a *fakeAgent) SetConfigOption(_, _, _ string) ([]antigravityacp.ConfigOption, error) {
	return fakeConfigOptions("model-a", "ask"), nil
}

func (a *fakeAgent) Cancel(string) { a.cancelled = true }

func (a *fakeAgent) CloseSession(string) { a.closed = true }

func fakeConfigOptions(model, mode string) []antigravityacp.ConfigOption {
	return []antigravityacp.ConfigOption{
		{ID: "model", Category: "model", CurrentValue: model, Options: []antigravityacp.OptionValue{{Value: "model-a", Name: "Model A"}, {Value: "model-b", Name: "Model B"}}},
		{ID: "mode", Category: "mode", CurrentValue: mode, Options: []antigravityacp.OptionValue{{Value: "ask", Name: "Ask"}, {Value: "auto", Name: "Auto"}}},
	}
}
