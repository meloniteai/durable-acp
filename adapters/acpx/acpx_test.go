package acpx

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

func TestAdapterAppliesChangedConfigurationBeforePrompt(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "calls.log")
	adapter := New(Config{
		Backend: "stub",
		Command: os.Args[0],
		Args:    []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(),
			"DURABLE_ACP_STUB_CHILD=1",
			"DURABLE_ACP_STUB_SESSION_MODES=1",
			"DURABLE_ACP_STUB_NO_PERMISSION=1",
			"DURABLE_ACP_STUB_LOG="+logPath,
		),
	})
	if _, err := adapter.StartSession(context.Background(), "configured", host.StartSessionRequest{
		Worktree:       t.TempDir(),
		Model:          "model-a",
		Reasoning:      "high",
		PermissionMode: "ask",
	}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.CloseSession("configured") })
	request := host.SendTurnRequest{Prompt: "one", Model: "model-b", Reasoning: "low", PermissionMode: "plan"}
	if _, err := adapter.SendTurn(context.Background(), "configured", request, nil); err != nil {
		t.Fatal(err)
	}
	request.Prompt = "two"
	if _, err := adapter.SendTurn(context.Background(), "configured", request, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(logPath) //nolint:gosec // Test-owned temporary path.
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Fields(string(raw)), []string{"config:model=model-b", "config:reasoning=low", "mode:plan", "prompt:one", "prompt:two"}; !slices.Equal(got, want) {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}

func TestAdapterConfigurationFailureAndUnknownControls(t *testing.T) {
	failedLog := filepath.Join(t.TempDir(), "failed.log")
	failed := New(Config{
		Backend: "stub",
		Command: os.Args[0],
		Args:    []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(),
			"DURABLE_ACP_STUB_CHILD=1",
			"DURABLE_ACP_STUB_NO_PERMISSION=1",
			"DURABLE_ACP_STUB_FAIL_CONFIG=model",
			"DURABLE_ACP_STUB_LOG="+failedLog,
		),
	})
	if _, err := failed.StartSession(context.Background(), "failed", host.StartSessionRequest{Worktree: t.TempDir()}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := failed.SendTurn(context.Background(), "failed", host.SendTurnRequest{Prompt: "blocked", Model: "model-b"}, nil); err == nil || !strings.Contains(err.Error(), "set session config") {
		t.Fatalf("configuration failure = %v", err)
	}
	if err := failed.CloseSession("failed"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(failedLog) //nolint:gosec // Test-owned temporary path.
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "prompt:") {
		t.Fatalf("prompt followed failed configuration: %q", raw)
	}

	unknown := New(Config{
		Backend: "stub",
		Command: os.Args[0],
		Args:    []string{"-test.run=TestACPChild", "--"},
		Environment: append(os.Environ(),
			"DURABLE_ACP_STUB_CHILD=1",
			"DURABLE_ACP_STUB_UNKNOWN_CONTROLS=1",
			"DURABLE_ACP_STUB_NO_PERMISSION=1",
		),
	})
	if _, err := unknown.StartSession(context.Background(), "unknown", host.StartSessionRequest{
		Worktree: t.TempDir(), Prompt: "best effort", Model: "model", Reasoning: "high", PermissionMode: "ask",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := unknown.CloseSession("unknown"); err != nil {
		t.Fatal(err)
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
	if os.Getenv("DURABLE_ACP_STUB_CHILD") != "1" {
		return
	}
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	model := "model-a"
	reasoning := "high"
	mode := "ask"
	for {
		var message rpcMessage
		if err := decoder.Decode(&message); err != nil {
			return
		}
		switch message.Method {
		case "initialize":
			writeRPC(t, encoder, message.ID, map[string]any{"protocolVersion": 1})
		case "session/new":
			response := map[string]any{"sessionId": "provider-1", "configOptions": childConfigOptionsFor(model, reasoning, mode)}
			addChildSessionModes(response, mode)
			writeRPC(t, encoder, message.ID, response)
		case "session/resume", "session/load":
			response := map[string]any{"configOptions": childConfigOptionsFor(model, reasoning, mode)}
			addChildSessionModes(response, mode)
			writeRPC(t, encoder, message.ID, response)
		case "session/set_config_option":
			var params struct {
				ConfigID string `json:"configId"`
				Value    string `json:"value"`
			}
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			childLog(t, "config:"+params.ConfigID+"="+params.Value)
			if os.Getenv("DURABLE_ACP_STUB_FAIL_CONFIG") == params.ConfigID {
				writeRPCError(t, encoder, message.ID, "configuration failed")
				continue
			}
			switch params.ConfigID {
			case "model":
				model = params.Value
			case "reasoning":
				reasoning = params.Value
			case "permission_mode":
				mode = params.Value
			}
			writeRPC(t, encoder, message.ID, map[string]any{"configOptions": childConfigOptionsFor(model, reasoning, mode)})
		case "session/set_mode":
			var params struct {
				ModeID string `json:"modeId"`
			}
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			childLog(t, "mode:"+params.ModeID)
			mode = params.ModeID
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
			if len(params.Prompt) > 0 {
				childLog(t, "prompt:"+params.Prompt[0].Text)
			}
			if os.Getenv("DURABLE_ACP_STUB_NO_PERMISSION") == "1" {
				writeRPC(t, encoder, message.ID, map[string]any{"stopReason": "end_turn"})
				continue
			}
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

func childConfigOptionsFor(model, reasoning, mode string) []map[string]any {
	if os.Getenv("DURABLE_ACP_STUB_UNKNOWN_CONTROLS") == "1" {
		return nil
	}
	options := []map[string]any{
		{"type": "select", "id": "model", "name": "Model", "category": "model", "currentValue": model, "options": []map[string]any{{"value": "model-a", "name": "Model A"}}},
		{"type": "select", "id": "reasoning", "name": "Reasoning", "category": "thought_level", "currentValue": reasoning, "options": []map[string]any{{"value": "high", "name": "High"}}},
	}
	if os.Getenv("DURABLE_ACP_STUB_SESSION_MODES") != "1" {
		options = append(options, map[string]any{"type": "select", "id": "permission_mode", "name": "Permission", "category": "mode", "currentValue": mode, "options": []map[string]any{{"value": "ask", "name": "Ask"}}})
	}
	return options
}

func addChildSessionModes(response map[string]any, current string) {
	if os.Getenv("DURABLE_ACP_STUB_SESSION_MODES") == "1" {
		response["modes"] = map[string]any{
			"currentModeId": current,
			"availableModes": []map[string]any{
				{"id": "ask", "name": "Ask"},
				{"id": "plan", "name": "Plan"},
			},
		}
	}
}

func childLog(t *testing.T, line string) {
	t.Helper()
	path := os.Getenv("DURABLE_ACP_STUB_LOG")
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // Test-owned log path.
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

func writeRPCError(t *testing.T, encoder *json.Encoder, id json.RawMessage, message string) {
	t.Helper()
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32000, "message": message}}); err != nil {
		t.Fatal(err)
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
