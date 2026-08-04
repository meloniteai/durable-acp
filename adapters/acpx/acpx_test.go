package acpx

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/conformance"
	"github.com/meloniteai/durable-acp/host"
)

func TestConfigOptionsAndCommandResolution(t *testing.T) {
	adapter := New(Config{Backend: " test ", Command: " ignored "}, WithCommand("  "), WithArgs("one", "two"), WithEnvironment([]string{"A=B"}), WithClientName(" client "))
	if adapter.Backend() != "test" || adapter.config.Command != "" || adapter.config.ClientName != "client" {
		t.Fatalf("config = %#v", adapter.config)
	}
	if got := adapter.Detect(context.Background()); got.Available || got.Error == "" || got.Backend != "test" {
		t.Fatalf("detect unavailable = %#v", got)
	}
	if got := New(Config{}).Backend(); got != "" {
		t.Fatalf("empty backend = %q", got)
	}
	if _, err := ResolveCommand(""); err == nil {
		t.Fatal("empty command resolved")
	}
	path := filepath.Join(t.TempDir(), "tool")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// #nosec G302 -- The test fixture must be executable by the current user.
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveCommand(path)
	if err != nil || resolved != path {
		t.Fatalf("resolved = %q, %v", resolved, err)
	}
	if _, err := ResolveCommand(t.TempDir()); err == nil {
		t.Fatal("directory resolved as executable")
	}
	if _, err := ResolveCommand("durable-acp-command-that-does-not-exist"); err == nil {
		t.Fatal("missing PATH command resolved")
	}
}

func TestACPXFormattingHelpers(t *testing.T) {
	blocks := promptBlocks(host.SendTurnRequest{Prompt: " hello ", Attachments: []host.Attachment{
		{MimeType: "image/png", DataBase64: "data"},
		{Path: "/tmp/file.txt"},
	}})
	if len(blocks) != 3 || contentText(blocks[0]) != "hello" || contentText(blocks[1]) != "[image]" || contentText(blocks[2]) != "file:///tmp/file.txt" {
		t.Fatalf("blocks = %#v", blocks)
	}
	if got := promptBlocks(host.SendTurnRequest{}); len(got) != 0 {
		t.Fatalf("empty blocks = %#v", got)
	}
	if childEnvironment(nil) != nil || len(childEnvironment([]string{"A=B"})) != 1 {
		t.Fatal("child environment did not preserve intended values")
	}
	if toolDisplay("", "", "", "") != nil {
		t.Fatal("empty display was not nil")
	}
	if got := toolDisplay("id", "title", "kind", "done"); got == nil || got.ID != "id" || got.Status != "done" {
		t.Fatalf("display = %#v", got)
	}
	if contentText(acp.AudioBlock("data", "audio/wav")) != "[audio]" {
		t.Fatal("content text missed non-text blocks")
	}
	if valueMap(make(chan int)) != nil {
		t.Fatal("unmarshalable value produced a map")
	}
	if got := valueMap(map[string]any{"value": true}); got["value"] != true {
		t.Fatalf("value map = %#v", got)
	}
}

func TestPermissionAndCatalogHelpers(t *testing.T) {
	options := []acp.PermissionOption{
		{OptionId: "allow", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "deny", Name: "Deny", Kind: acp.PermissionOptionKindRejectOnce},
	}
	if got := choosePermissionOption(host.InteractionResponse{OptionID: "allow"}, options); got != "allow" {
		t.Fatalf("explicit option = %q", got)
	}
	if got := choosePermissionOption(host.InteractionResponse{Action: "approve"}, options); got != "allow" {
		t.Fatalf("approve option = %q", got)
	}
	if got := choosePermissionOption(host.InteractionResponse{Action: "deny"}, options); got != "deny" {
		t.Fatalf("deny option = %q", got)
	}
	if got := choosePermissionOption(host.InteractionResponse{OptionID: "unknown"}, options); got != "" {
		t.Fatalf("unknown option = %q", got)
	}
	title := "Use tool"
	if got := permissionTitle(acp.ToolCallUpdate{Title: &title}); got != title {
		t.Fatalf("permission title = %q", got)
	}
	if got := permissionTitle(acp.ToolCallUpdate{}); got != "Permission requested" {
		t.Fatalf("fallback title = %q", got)
	}
	if got := permissionOptions(options); len(got) != 2 || got[0].ID != "allow" {
		t.Fatalf("permission options = %#v", got)
	}

	category := acp.SessionConfigOptionCategory("model")
	choices := acp.SessionConfigSelectOptionsUngrouped{{Value: "model-a", Name: "Model A"}}
	config := []acp.SessionConfigOption{{Select: &acp.SessionConfigOptionSelect{Id: "model", Category: &category, Options: acp.SessionConfigSelectOptions{Ungrouped: &choices}}}}
	if got := findOption(config, "model"); got != "model" {
		t.Fatalf("findOption = %q", got)
	}
	catalog := catalogFromConfig(config, nil)
	if len(catalog.Models) != 1 || catalog.Models[0].ID != "model-a" {
		t.Fatalf("catalog = %#v", catalog)
	}
	if got := selectOptions(acp.SessionConfigSelectOptions{}); got != nil {
		t.Fatalf("empty choices = %#v", got)
	}
}

func TestAdapterSessionValidation(t *testing.T) {
	if _, err := (*Adapter)(nil).session("x"); err == nil {
		t.Fatal("nil adapter session succeeded")
	}
	adapter := New(Config{Backend: "test"})
	if _, err := adapter.session("x"); err == nil {
		t.Fatal("missing session succeeded")
	}
	if err := adapter.CloseSession("x"); err != nil {
		t.Fatal(err)
	}
	if err := (*Adapter)(nil).CloseSession("x"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.RespondInteraction(context.Background(), "x", host.InteractionResponse{}); err == nil {
		t.Fatal("missing interaction session response succeeded")
	}
	if _, err := (*Adapter)(nil).Catalog(context.Background()); err == nil {
		t.Fatal("nil catalog succeeded")
	}
}

func TestManagedSessionElicitationAndUpdates(t *testing.T) {
	adapter := New(Config{Backend: "test"})
	events := make(chan host.Event, 32)
	managed := &managedSession{
		adapter:      adapter,
		hostID:       "host",
		backendID:    "provider",
		emit:         func(event host.Event) { events <- event },
		interactions: map[string]chan host.InteractionResponse{},
		done:         make(chan struct{}),
	}
	adapter.sessions["host"] = managed
	if err := managed.SessionUpdate(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	formResult := make(chan *acp.CreateElicitationResponse, 1)
	go func() {
		response, err := managed.CreateElicitation(context.Background(), &acp.CreateElicitationRequest{Form: &acp.CreateElicitationForm{Message: "name"}})
		if err != nil {
			t.Errorf("CreateElicitation: %v", err)
			return
		}
		formResult <- response
	}()
	interaction := <-events
	if interaction.Type != host.EventInteractionRequested || interaction.Interaction == nil || interaction.Interaction.Kind != host.InteractionForm {
		t.Fatalf("form interaction = %#v", interaction)
	}
	if err := adapter.RespondInteraction(context.Background(), "host", host.InteractionResponse{RequestID: interaction.Interaction.ID, Action: "submit", Values: map[string]any{"name": "Ada"}}); err != nil {
		t.Fatal(err)
	}
	if response := <-formResult; response.Accept == nil || response.Accept.Content["name"] != "Ada" {
		t.Fatalf("form response = %#v", response)
	}

	urlResult := make(chan *acp.CreateElicitationResponse, 1)
	go func() {
		response, err := managed.CreateElicitation(context.Background(), &acp.CreateElicitationRequest{Url: &acp.CreateElicitationUrl{Message: "sign in"}})
		if err == nil {
			urlResult <- response
		}
	}()
	interaction = <-events
	if interaction.Interaction == nil || interaction.Interaction.Kind != host.InteractionChoice {
		t.Fatalf("URL interaction = %#v", interaction)
	}
	if err := adapter.RespondInteraction(context.Background(), "host", host.InteractionResponse{RequestID: interaction.Interaction.ID, Action: "decline"}); err != nil {
		t.Fatal(err)
	}
	if response := <-urlResult; response.Decline == nil {
		t.Fatalf("URL response = %#v", response)
	}

	permissionResult := make(chan *acp.RequestPermissionResponse, 1)
	go func() {
		response, err := managed.RequestPermission(context.Background(), &acp.RequestPermissionRequest{Options: []acp.PermissionOption{{OptionId: "allow", Kind: acp.PermissionOptionKindAllowOnce}}})
		if err == nil {
			permissionResult <- response
		}
	}()
	interaction = <-events
	if err := adapter.RespondInteraction(context.Background(), "host", host.InteractionResponse{RequestID: interaction.Interaction.ID, Action: "approve"}); err != nil {
		t.Fatal(err)
	}
	if response := <-permissionResult; response.Outcome.Selected == nil || response.Outcome.Selected.OptionId != "allow" {
		t.Fatalf("permission response = %#v", response)
	}
	if _, err := managed.CreateElicitation(context.Background(), nil); err == nil {
		t.Fatal("nil elicitation succeeded")
	}
	if _, err := managed.RequestPermission(context.Background(), nil); err == nil {
		t.Fatal("nil permission succeeded")
	}

	content := acp.TextBlock("text")
	status := acp.ToolCallStatus("completed")
	title := "updated"
	kind := acp.ToolKindExecute
	category := acp.SessionConfigOptionCategory("model")
	choices := acp.SessionConfigSelectOptionsUngrouped{{Value: "model", Name: "Model"}}
	option := acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{Id: "model", Category: &category, Options: acp.SessionConfigSelectOptions{Ungrouped: &choices}}}
	updates := []acp.SessionUpdate{
		{UserMessageChunk: &acp.SessionUpdateUserMessageChunk{Content: content}},
		{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: content}},
		{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{Content: content}},
		{ToolCall: &acp.SessionUpdateToolCall{ToolCallId: "tool", Title: "run", Kind: kind, Status: status}},
		{ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "tool", Title: &title, Kind: &kind, Status: &status}},
		{Plan: &acp.SessionUpdatePlan{}},
		{AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: []acp.AvailableCommand{{Name: "review", Description: "Review"}}}},
		{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: []acp.SessionConfigOption{option}}},
		{CurrentModeUpdate: &acp.SessionCurrentModeUpdate{CurrentModeId: "ask"}},
		{UsageUpdate: &acp.SessionUsageUpdate{Size: 10, Used: 2}},
	}
	for _, update := range updates {
		managed.emitUpdate(update)
	}
	seen := map[host.EventType]bool{}
	for len(events) > 0 {
		seen[(<-events).Type] = true
	}
	for _, kind := range []host.EventType{host.EventMessage, host.EventThinking, host.EventToolStarted, host.EventToolOutput, host.EventPlanUpdate, host.EventAvailableCommands, host.EventConfigCatalog, host.EventModels, host.EventTraceUpdated} {
		if !seen[kind] {
			t.Fatalf("missing update event %q in %#v", kind, seen)
		}
	}
	managed.emitConfig([]acp.SessionConfigOption{option}, &acp.SessionModeState{AvailableModes: []acp.SessionMode{{Id: "ask", Name: "Ask"}}})
	managed.stop()
	managed.cancelInteractions()
	if err := adapter.RespondInteraction(context.Background(), "host", host.InteractionResponse{RequestID: "missing"}); err == nil {
		t.Fatal("missing interaction response succeeded")
	}
}

func TestAdapterLifecycleAndPermissionRoundTrip(t *testing.T) {
	adapter := New(Config{
		Backend:     "stub",
		Command:     os.Args[0],
		Args:        []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(), "DURABLE_ACP_STUB_CHILD=1"),
	})
	events := make(chan host.Event, 16)
	state, err := adapter.StartSession(context.Background(), "host-1", host.StartSessionRequest{
		Worktree: t.TempDir(),
	}, func(event host.Event) { events <- event })
	if err != nil {
		t.Fatal(err)
	}
	if state.ID != "provider-1" {
		t.Fatalf("session state = %+v", state)
	}

	result := make(chan error, 1)
	go func() {
		_, err := adapter.SendTurn(context.Background(), "host-1", host.SendTurnRequest{Prompt: "hello"}, nil)
		result <- err
	}()

	request := waitForEvent(t, events, host.EventInteractionRequested)
	if request.Interaction == nil || request.Interaction.Kind != host.InteractionPermission {
		t.Fatalf("interaction = %+v", request)
	}
	if err := adapter.RespondInteraction(context.Background(), "host-1", host.InteractionResponse{
		RequestID: request.Interaction.ID,
		Action:    "approve",
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if message := waitForEvent(t, events, host.EventMessage); message.Message != "approved" {
		t.Fatalf("message = %+v", message)
	}
	_ = waitForEvent(t, events, host.EventTurnComplete)
	if err := adapter.CloseSession("host-1"); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterSatisfiesReusableConformance(t *testing.T) {
	adapter := New(Config{
		Backend:     "stub",
		Command:     os.Args[0],
		Args:        []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(), "DURABLE_ACP_STUB_CHILD=1"),
	})
	conformance.RunAdapter(t, adapter, conformance.AdapterConfig{
		Worktree:           t.TempDir(),
		RequireInteraction: true,
	})
}

func TestAdapterCatalogConfigurationAndResume(t *testing.T) {
	var diagnostics strings.Builder
	adapter := New(Config{
		Backend:     "stub",
		Command:     os.Args[0],
		Args:        []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(), "DURABLE_ACP_STUB_CHILD=1"),
	}, WithStderr(&diagnostics))
	catalog, err := adapter.Catalog(context.Background())
	if err != nil || len(catalog.Models) != 1 || len(catalog.PermissionModes) != 1 || len(catalog.Reasoning) != 1 {
		t.Fatalf("catalog = %#v, %v", catalog, err)
	}
	state, err := adapter.StartSession(context.Background(), "resume", host.StartSessionRequest{
		Worktree:               t.TempDir(),
		ResumeBackendSessionID: "provider-1",
		Model:                  "model-a",
		Reasoning:              "high",
		PermissionMode:         "ask",
	}, nil)
	if err != nil || state.ID != "provider-1" {
		t.Fatalf("resume = %#v, %v", state, err)
	}
	if _, duplicateErr := adapter.StartSession(context.Background(), "resume", host.StartSessionRequest{Worktree: t.TempDir()}, nil); duplicateErr == nil {
		t.Fatal("StartSession accepted a duplicate host session")
	}
	events := make(chan host.Event, 16)
	restarted, err := adapter.RestartSession(context.Background(), "resume", func(event host.Event) { events <- event })
	if err != nil || restarted.ID != state.ID {
		t.Fatalf("restart = %#v, %v", restarted, err)
	}
	if recovered := waitForEvent(t, events, host.EventAgentRecovered); recovered.BackendSessionID != state.ID {
		t.Fatalf("recovered event = %#v", recovered)
	}
	if err := adapter.CloseSession("resume"); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterAppliesChangedConfigurationBeforeTurns(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "config.trace")
	adapter := New(Config{
		Backend: "stub", Command: os.Args[0], Args: []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(), "DURABLE_ACP_CONFIG_CHILD=1", "DURABLE_ACP_CONFIG_TRACE="+trace),
	})
	state, err := adapter.StartSession(context.Background(), "config", host.StartSessionRequest{
		Worktree: t.TempDir(), Model: "model-b", Reasoning: "high", PermissionMode: "auto",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, sendErr := adapter.SendTurn(context.Background(), "config", host.SendTurnRequest{
		Prompt: "one", Model: "model-b", Reasoning: "high", PermissionMode: "auto",
	}, nil); sendErr != nil {
		t.Fatal(sendErr)
	}
	if _, sendErr := adapter.SendTurn(context.Background(), "config", host.SendTurnRequest{
		Prompt: "two", Model: "model-b", Reasoning: "low", PermissionMode: "plan",
	}, nil); sendErr != nil {
		t.Fatal(sendErr)
	}
	if closeErr := adapter.CloseSession("config"); closeErr != nil {
		t.Fatal(closeErr)
	}
	// #nosec G304 -- trace is a test-owned path below t.TempDir.
	raw, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Fields(string(raw))
	want := []string{"config:model=model-b", "config:reasoning=high", "mode:auto", "prompt:one", "config:reasoning=low", "mode:plan", "prompt:two"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("configuration trace = %#v, want %#v", got, want)
	}
	if state.ID != "provider-config" {
		t.Fatalf("state = %#v", state)
	}
}

func TestAdapterTreatsUnadvertisedConfigurationAsOptional(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "config.trace")
	adapter := New(Config{
		Backend: "stub", Command: os.Args[0], Args: []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(), "DURABLE_ACP_CONFIG_CHILD=1", "DURABLE_ACP_CONFIG_EMPTY=1", "DURABLE_ACP_CONFIG_TRACE="+trace),
	})
	if _, err := adapter.StartSession(context.Background(), "optional", host.StartSessionRequest{
		Worktree: t.TempDir(), Model: "unknown-model", Reasoning: "unknown-reasoning", PermissionMode: "unknown-mode",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.SendTurn(context.Background(), "optional", host.SendTurnRequest{
		Prompt: "optional", Model: "unknown-model", Reasoning: "unknown-reasoning", PermissionMode: "unknown-mode",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := adapter.CloseSession("optional"); err != nil {
		t.Fatal(err)
	}
	// #nosec G304 -- trace is a test-owned path below t.TempDir.
	raw, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "prompt:optional" {
		t.Fatalf("configuration trace = %q", got)
	}
}

func TestAdapterReturnsAdvertisedConfigurationFailures(t *testing.T) {
	adapter := New(Config{
		Backend: "stub", Command: os.Args[0], Args: []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(), "DURABLE_ACP_CONFIG_CHILD=1", "DURABLE_ACP_CONFIG_FAIL=1"),
	})
	_, err := adapter.StartSession(context.Background(), "failure", host.StartSessionRequest{Worktree: t.TempDir(), Model: "model-b"}, nil)
	if err == nil || !strings.Contains(err.Error(), "set model") || !strings.Contains(err.Error(), "provider rejected configuration") {
		t.Fatalf("configuration error = %v", err)
	}
}

func waitForEvent(t *testing.T, events <-chan host.Event, kind host.EventType) host.Event {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-events:
			if event.Type == kind {
				return event
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

func TestACPChild(t *testing.T) {
	if os.Getenv("DURABLE_ACP_CONFIG_CHILD") == "1" {
		runConfigurationChild(t)
		return
	}
	if os.Getenv("DURABLE_ACP_STUB_CHILD") != "1" {
		return
	}
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	for {
		var message rpcMessage
		if err := decoder.Decode(&message); err != nil {
			return
		}
		switch message.Method {
		case "initialize":
			writeRPC(t, encoder, message.ID, map[string]any{"protocolVersion": 1})
		case "session/new":
			writeRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider-1", "configOptions": childConfigOptions()})
		case "session/resume", "session/load":
			writeRPC(t, encoder, message.ID, map[string]any{"configOptions": childConfigOptions()})
		case "session/set_config_option":
			writeRPC(t, encoder, message.ID, map[string]any{"configOptions": childConfigOptions()})
		case "session/prompt":
			permissionID := json.RawMessage("71")
			if err := encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      permissionID,
				"method":  "session/request_permission",
				"params": map[string]any{
					"sessionId": "provider-1",
					"toolCall":  map[string]any{"toolCallId": "tool-1"},
					"options": []map[string]any{
						{"optionId": "allow", "name": "Allow", "kind": "allow_once"},
						{"optionId": "deny", "name": "Deny", "kind": "reject_once"},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			var permissionResponse rpcMessage
			if err := decoder.Decode(&permissionResponse); err != nil {
				t.Fatal(err)
			}
			if string(permissionResponse.ID) != string(permissionID) || !strings.Contains(string(permissionResponse.Result), "allow") {
				t.Fatalf("permission response = %+v", permissionResponse)
			}
			if err := encoder.Encode(map[string]any{
				"jsonrpc": "2.0",
				"method":  "session/update",
				"params": map[string]any{
					"sessionId": "provider-1",
					"update": map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "approved"},
					},
				},
			}); err != nil {
				t.Fatal(err)
			}
			writeRPC(t, encoder, message.ID, map[string]any{"stopReason": "end_turn"})
		}
	}
}

func childConfigOptions() []map[string]any {
	return []map[string]any{
		{"type": "select", "id": "model", "name": "Model", "category": "model", "currentValue": "model-a", "options": []map[string]any{{"value": "model-a", "name": "Model A"}}},
		{"type": "select", "id": "reasoning", "name": "Reasoning", "category": "thought_level", "currentValue": "high", "options": []map[string]any{{"value": "high", "name": "High"}}},
		{"type": "select", "id": "permission_mode", "name": "Permission", "category": "mode", "currentValue": "ask", "options": []map[string]any{{"value": "ask", "name": "Ask"}}},
	}
}

type rpcMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
}

func writeRPC(t *testing.T, encoder *json.Encoder, id json.RawMessage, result any) {
	t.Helper()
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result}); err != nil {
		t.Fatal(err)
	}
}

func runConfigurationChild(t *testing.T) {
	t.Helper()
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	model := "model-a"
	reasoning := "low"
	mode := "ask"
	configOptions := func() []map[string]any {
		if os.Getenv("DURABLE_ACP_CONFIG_EMPTY") == "1" {
			return nil
		}
		return []map[string]any{
			{"type": "select", "id": "model", "name": "Model", "category": "model", "currentValue": model, "options": []map[string]any{{"value": "model-a", "name": "Model A"}, {"value": "model-b", "name": "Model B"}}},
			{"type": "select", "id": "reasoning", "name": "Reasoning", "category": "thought_level", "currentValue": reasoning, "options": []map[string]any{{"value": "low", "name": "Low"}, {"value": "high", "name": "High"}}},
		}
	}
	modes := func() any {
		if os.Getenv("DURABLE_ACP_CONFIG_EMPTY") == "1" {
			return nil
		}
		return map[string]any{
			"currentModeId":  mode,
			"availableModes": []map[string]any{{"id": "ask", "name": "Ask"}, {"id": "auto", "name": "Auto"}, {"id": "plan", "name": "Plan"}},
		}
	}
	trace := func(line string) {
		path := os.Getenv("DURABLE_ACP_CONFIG_TRACE")
		if path == "" {
			return
		}
		// #nosec G304,G703 -- the parent test supplies the isolated trace path.
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintln(file, line); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for {
		var message rpcMessage
		if err := decoder.Decode(&message); err != nil {
			return
		}
		switch message.Method {
		case "initialize":
			writeRPC(t, encoder, message.ID, map[string]any{"protocolVersion": 1})
		case "session/new", "session/resume", "session/load":
			writeRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider-config", "configOptions": configOptions(), "modes": modes()})
		case "session/set_config_option":
			if os.Getenv("DURABLE_ACP_CONFIG_FAIL") == "1" {
				if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": message.ID, "error": map[string]any{"code": -32603, "message": "provider rejected configuration"}}); err != nil {
					t.Fatal(err)
				}
				continue
			}
			var params struct {
				ConfigID string `json:"configId"`
				Value    string `json:"value"`
			}
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			switch params.ConfigID {
			case "model":
				model = params.Value
			case "reasoning":
				reasoning = params.Value
			}
			trace("config:" + params.ConfigID + "=" + params.Value)
			writeRPC(t, encoder, message.ID, map[string]any{"configOptions": configOptions()})
		case "session/set_mode":
			var params struct {
				ModeID string `json:"modeId"`
			}
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			mode = params.ModeID
			trace("mode:" + mode)
			writeRPC(t, encoder, message.ID, map[string]any{})
		case "session/prompt":
			var params struct {
				Prompt []struct {
					Text string `json:"text"`
				} `json:"prompt"`
			}
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			text := ""
			if len(params.Prompt) > 0 {
				text = params.Prompt[0].Text
			}
			trace("prompt:" + text)
			writeRPC(t, encoder, message.ID, map[string]any{"stopReason": "end_turn"})
		}
	}
}
