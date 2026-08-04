package acpx

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/client"
	"github.com/meloniteai/durable-acp/conformance"
	"github.com/meloniteai/durable-acp/host"
)

func TestConfigOptionsAndCommandResolution(t *testing.T) {
	capabilities := acp.ClientCapabilities{Terminal: true}
	adapter := New(Config{Backend: " test ", Command: " ignored "},
		WithCommand("  "),
		WithArgs("one", "two"),
		WithEnvironment([]string{"A=B"}),
		WithClientName("ignored"),
		WithClientInfo(" client ", " Client ", " 2 "),
		WithClientCapabilities(capabilities),
		WithLoadSessionFirst(true),
		WithRestartOnExit(true),
		WithLegacyExtensions(true),
		WithBestEffortConfiguration(true),
		WithSessionModeValues("ask", "auto"),
		WithDoneCompletionGrace(time.Second),
	)
	if adapter.Backend() != "test" || adapter.config.Command != "" || adapter.config.ClientName != "client" || adapter.config.ClientTitle != "Client" || adapter.config.ClientVersion != "2" {
		t.Fatalf("config = %#v", adapter.config)
	}
	if adapter.config.ClientCapabilities == nil || !adapter.config.ClientCapabilities.Terminal || !adapter.config.LoadSessionFirst || !adapter.config.RestartOnExit || !adapter.config.LegacyExtensions {
		t.Fatalf("extended config = %#v", adapter.config)
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

func TestAdapterDirectOperations(t *testing.T) {
	worktree := t.TempDir()
	adapter := New(Config{
		Backend: "stub", Command: os.Args[0], Args: []string{"-test.run=TestACPChild", "--"},
		Environment:      append(os.Environ(), "DURABLE_ACP_RECOVERY_CHILD=1", "DURABLE_ACP_RECOVERY_TRACE="+filepath.Join(t.TempDir(), "trace")),
		LegacyExtensions: true,
	})
	if _, err := adapter.StartSession(context.Background(), "direct", host.StartSessionRequest{Worktree: worktree}, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.CloseSession("direct") }()
	if directory, err := adapter.SessionDirectory("direct"); err != nil || directory != worktree {
		t.Fatalf("session directory = %q, %v", directory, err)
	}
	if backendID, err := adapter.BackendSessionID("direct"); err != nil || backendID != "provider-recovery" {
		t.Fatalf("backend session ID = %q, %v", backendID, err)
	}
	if turnID, err := adapter.ActiveTurnID("direct"); err != nil || turnID != "" {
		t.Fatalf("active turn ID = %q, %v", turnID, err)
	}
	raw, callErr := adapter.CallSession(context.Background(), "direct", "provider/echo", map[string]any{"value": "ok"})
	if callErr != nil || !strings.Contains(string(raw), `"sessionId":"provider-recovery"`) || !strings.Contains(string(raw), `"value":"ok"`) {
		t.Fatalf("provider call = %s, %v", raw, callErr)
	}
	if err := adapter.PromptSession(context.Background(), "direct", "", "prompt"); err == nil {
		t.Fatal("prompt accepted an empty backend session ID")
	}
	if err := adapter.PromptSession(context.Background(), "direct", "provider-recovery", " "); err == nil {
		t.Fatal("prompt accepted empty content")
	}
	if err := adapter.PromptSession(context.Background(), "direct", "provider-recovery", "direct"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.PromptForkSession(context.Background(), "direct", "provider-child", "fork", []host.ForkMCPServer{{Name: "review"}, {Name: " "}}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.CloseBackendSession(context.Background(), "direct", "provider-child"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.StopSessionProcess("direct"); err != nil {
		t.Fatal(err)
	}

	unsupported := New(Config{Backend: "stub"})
	unsupported.sessions["direct"] = adapter.sessions["direct"]
	_, unsupportedErr := unsupported.CallSession(context.Background(), "direct", "provider/echo", nil)
	var operationErr *UnsupportedOperationError
	if !errors.As(unsupportedErr, &operationErr) || !errors.Is(unsupportedErr, ErrUnsupportedOperation) || operationErr.Error() == "" {
		t.Fatalf("unsupported operation error = %v", unsupportedErr)
	}
	for _, call := range []func() error{
		func() error { _, err := adapter.SessionDirectory("missing"); return err },
		func() error { _, err := adapter.BackendSessionID("missing"); return err },
		func() error { _, err := adapter.ActiveTurnID("missing"); return err },
		func() error { return adapter.StopSessionProcess("missing") },
		func() error { return adapter.PromptSession(context.Background(), "missing", "provider", "prompt") },
		func() error {
			return adapter.PromptForkSession(context.Background(), "missing", "provider", "prompt", nil)
		},
		func() error { return adapter.CloseBackendSession(context.Background(), "missing", "provider") },
	} {
		if err := call(); err == nil {
			t.Fatal("missing session operation succeeded")
		}
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

	request := &acp.CreateElicitationRequest{Form: &acp.CreateElicitationForm{RequestedSchema: acp.ElicitationSchema{
		Required: []string{"choice"},
		Properties: map[string]any{
			"note": map[string]any{"type": "string", "title": "Note"},
			"choice": map[string]any{"type": "string", "description": "Pick one", "oneOf": []any{
				map[string]any{"const": "a", "title": "A", "description": "first"},
				map[string]any{"const": "b", "title": "B"},
			}},
		},
	}}}
	fields := elicitationFields(request)
	if len(fields) != 2 || fields[0].ID != "choice" || !fields[0].Required || len(fields[0].Options) != 2 || fields[1].ID != "note" || !fields[1].AllowFreeText {
		t.Fatalf("elicitation fields = %#v", fields)
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

func TestManagedSessionDoneCompletion(t *testing.T) {
	adapter := New(Config{Backend: "cursor", CompleteOnDone: true, DoneCompletionGrace: 10 * time.Millisecond})
	events := make(chan host.Event, 4)
	managed := &managedSession{
		adapter: adapter, backendID: "provider", emit: func(event host.Event) { events <- event },
		interactions: map[string]*pendingInteraction{}, done: make(chan struct{}),
	}
	done := json.RawMessage(`{"method":"session/update","params":{"sessionId":"provider","update":{"sessionUpdate":"done"}}}`)
	if err := managed.observe(context.Background(), client.DirectionInbound, done); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("inactive done event = %#v", event)
	case <-time.After(25 * time.Millisecond):
	}

	turnContext, cancel := context.WithCancel(context.Background())
	managed.setTurn("provider:1", cancel)
	if err := managed.observe(context.Background(), client.DirectionInbound, done); err != nil {
		t.Fatal(err)
	}
	event := waitForEvent(t, events, host.EventTurnComplete)
	if event.BackendTurnID != "provider:1" || event.Data["stop_reason"] != "done" {
		t.Fatalf("done completion = %#v", event)
	}
	select {
	case <-turnContext.Done():
	case <-time.After(time.Second):
		t.Fatal("done completion did not cancel the prompt")
	}

	managed.setTurn("provider:2", func() {})
	managed.toolMu.Lock()
	managed.toolActive = true
	managed.toolMu.Unlock()
	if err := managed.observe(context.Background(), client.DirectionInbound, done); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("tool-active done event = %#v", event)
	case <-time.After(25 * time.Millisecond):
	}
	if managed.currentTurnID() != "provider:2" {
		t.Fatalf("tool-active done cleared turn %q", managed.currentTurnID())
	}
}

func TestManagedSessionLegacyPlanUpdates(t *testing.T) {
	adapter := New(Config{Backend: "test"})
	events := make(chan host.Event, 3)
	managed := &managedSession{
		adapter: adapter, backendID: "provider", emit: func(event host.Event) { events <- event },
		interactions: map[string]*pendingInteraction{}, done: make(chan struct{}),
	}
	updates := []json.RawMessage{
		json.RawMessage(`{"method":"session/update","params":{"sessionId":"provider","update":{"sessionUpdate":"plan_update","plan":{"type":"markdown","planId":"plan-1","content":"1. Inspect\n2. Change"}}}}`),
		json.RawMessage(`{"method":"session/update","params":{"sessionId":"provider","update":{"sessionUpdate":"plan_removed"}}}`),
		json.RawMessage(`{"method":"session/update","params":{"sessionId":"provider","update":{"sessionUpdate":"error","message":"failed"}}}`),
	}
	for _, update := range updates {
		if err := managed.observe(context.Background(), client.DirectionInbound, update); err != nil {
			t.Fatal(err)
		}
	}
	first := <-events
	if first.Type != host.EventPlanUpdate || first.Message != "1. Inspect\n2. Change" || first.BackendSessionID != "provider" {
		t.Fatalf("plan update = %#v", first)
	}
	second := <-events
	if second.Type != host.EventPlanUpdate || second.Message != "Plan removed." {
		t.Fatalf("plan removal = %#v", second)
	}
	third := <-events
	if third.Type != host.EventTurnFailed || third.Message != "failed" {
		t.Fatalf("error update = %#v", third)
	}
}

func TestManagedSessionPreservesWhitespaceChunks(t *testing.T) {
	adapter := New(Config{Backend: "test"})
	events := make(chan host.Event, 2)
	managed := &managedSession{
		adapter: adapter, backendID: "provider", emit: func(event host.Event) { events <- event },
		interactions: map[string]*pendingInteraction{}, done: make(chan struct{}),
	}
	managed.emitUpdate(acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("\n\n")}})
	managed.emitUpdate(acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{Content: acp.TextBlock(" \n")}})
	message := <-events
	if message.Type != host.EventMessage || message.Message != "\n\n" || message.Data["delta"] != "\n\n" {
		t.Fatalf("message chunk = %#v", message)
	}
	thought := <-events
	if thought.Type != host.EventThinking || thought.Message != " \n" || thought.Data["delta"] != " \n" {
		t.Fatalf("thought chunk = %#v", thought)
	}
}

func TestManagedSessionForkInteractionPolicy(t *testing.T) {
	adapter := New(Config{Backend: "test"})
	managed := &managedSession{
		adapter: adapter, backendID: "parent", forks: map[string]*forkInteractionPolicy{
			"child": {allowedToolPrefixes: []string{"mcp__review__"}, tools: map[string]string{}},
		},
	}
	if err := managed.SessionUpdate(context.Background(), &acp.SessionNotification{
		SessionId: "child",
		Update: acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
			ToolCallId: "tool-1", Title: "Review", Meta: map[string]any{"claudeCode": map[string]any{"toolName": "mcp__review__check"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	request := &acp.RequestPermissionRequest{
		SessionId: "child", ToolCall: acp.ToolCallUpdate{ToolCallId: "tool-1"},
		Options: []acp.PermissionOption{{OptionId: "allow", Kind: acp.PermissionOptionKindAllowOnce}},
	}
	response, err := managed.RequestPermission(context.Background(), request)
	if err != nil || response.Outcome.Selected == nil || response.Outcome.Selected.OptionId != "allow" {
		t.Fatalf("allowed fork permission = %#v, %v", response, err)
	}
	request.ToolCall.ToolCallId = "unknown"
	response, err = managed.RequestPermission(context.Background(), request)
	if err != nil || response.Outcome.Cancelled == nil {
		t.Fatalf("unknown fork permission = %#v, %v", response, err)
	}
	request.ToolCall.ToolCallId = "tool-1"
	request.Options = nil
	response, err = managed.RequestPermission(context.Background(), request)
	if err != nil || response.Outcome.Cancelled == nil {
		t.Fatalf("optionless fork permission = %#v, %v", response, err)
	}
	if prefixes := policyPrefixes(nil); prefixes != nil {
		t.Fatalf("nil policy prefixes = %#v", prefixes)
	}
	managed.observeForkUpdate("child", acp.SessionUpdate{})
}

func TestCatalogCollectorCommands(t *testing.T) {
	collector := &catalogCollector{}
	if err := collector.SessionUpdate(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := collector.SessionUpdate(context.Background(), &acp.SessionNotification{}); err != nil {
		t.Fatal(err)
	}
	if err := collector.SessionUpdate(context.Background(), &acp.SessionNotification{Update: acp.SessionUpdate{
		AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: []acp.AvailableCommand{{
			Name: "review", Description: "Review", Input: &acp.AvailableCommandInput{Unstructured: &acp.UnstructuredCommandInput{Hint: "path"}},
		}}},
	}}); err != nil {
		t.Fatal(err)
	}
	commands := collector.commands()
	if len(commands) != 1 || commands[0].Name != "review" || commands[0].InputHint != "path" {
		t.Fatalf("commands = %#v", commands)
	}
}

func TestManagedSessionTracksFileChangeLocationAcrossUpdates(t *testing.T) {
	adapter := New(Config{Backend: "test"})
	events := make(chan host.Event, 4)
	managed := &managedSession{
		adapter: adapter, backendID: "provider", emit: func(event host.Event) { events <- event },
		interactions: map[string]*pendingInteraction{}, done: make(chan struct{}),
	}
	kind := acp.ToolKindEdit
	inProgress := acp.ToolCallStatus("in_progress")
	completed := acp.ToolCallStatus("completed")
	managed.emitUpdate(acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{ToolCallId: "edit-1", Title: "Edit file", Kind: kind, Status: inProgress}})
	managed.emitUpdate(acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "edit-1", Locations: []acp.ToolCallLocation{{Path: "/tmp/file.txt"}}}})
	managed.emitUpdate(acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "edit-1", Status: &completed}})
	var changed []host.Event
	for len(events) > 0 {
		event := <-events
		if event.Type == host.EventFileChanged {
			changed = append(changed, event)
		}
	}
	if len(changed) != 1 || changed[0].Data["path"] != "/tmp/file.txt" || changed[0].Data["tool_call_id"] != "edit-1" {
		t.Fatalf("file changes = %#v", changed)
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
		interactions: map[string]*pendingInteraction{},
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
	if resolved := <-events; resolved.Type != host.EventInteractionResolved {
		t.Fatalf("form resolved event = %#v", resolved)
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
	if resolved := <-events; resolved.Type != host.EventInteractionResolved {
		t.Fatalf("URL resolved event = %#v", resolved)
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
	if resolved := <-events; resolved.Type != host.EventInteractionResolved {
		t.Fatalf("permission resolved event = %#v", resolved)
	}

	extensionResult := make(chan any, 1)
	go func() {
		response, extensionErr := managed.ExtensionRequest(context.Background(), "provider/question", json.RawMessage(`{"question":"Continue?"}`))
		if extensionErr != nil {
			t.Errorf("ExtensionRequest: %v", extensionErr)
			return
		}
		extensionResult <- response
	}()
	interaction = <-events
	if interaction.Interaction == nil || interaction.Interaction.Kind != host.InteractionChoice {
		t.Fatalf("extension interaction = %#v", interaction)
	}
	if err := adapter.RespondInteraction(context.Background(), "host", host.InteractionResponse{RequestID: interaction.Interaction.ID, Action: "answer", Values: map[string]any{"outcome": "answered"}}); err != nil {
		t.Fatal(err)
	}
	if response := <-extensionResult; !reflect.DeepEqual(response, map[string]any{"outcome": "answered"}) {
		t.Fatalf("extension response = %#v", response)
	}
	if resolved := <-events; resolved.Type != host.EventInteractionResolved {
		t.Fatalf("extension resolved event = %#v", resolved)
	}
	if err := managed.ElicitationComplete(context.Background(), &acp.CompleteElicitationNotification{}); err != nil {
		t.Fatal(err)
	}
	if err := managed.ExtensionNotification(context.Background(), "provider/update", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := managed.CreateElicitation(context.Background(), nil); err == nil {
		t.Fatal("nil elicitation succeeded")
	}
	if _, err := managed.RequestPermission(context.Background(), nil); err == nil {
		t.Fatal("nil permission succeeded")
	}

	content := acp.TextBlock("text")
	image := acp.ImageBlock("image-data", "image/png")
	toolContent := []acp.ToolCallContent{{Content: &acp.ToolCallContentContent{Content: image}}}
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
		{ToolCall: &acp.SessionUpdateToolCall{ToolCallId: "tool", Title: "run", Kind: kind, Status: status, Content: toolContent}},
		{ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "tool", Title: &title, Kind: &kind, Status: &status, Content: toolContent}},
		{Plan: &acp.SessionUpdatePlan{Entries: []acp.PlanEntry{{Content: "Review", Status: acp.PlanEntryStatusPending}}}},
		{AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: []acp.AvailableCommand{{Name: "review", Description: "Review", Input: &acp.AvailableCommandInput{Unstructured: &acp.UnstructuredCommandInput{Hint: "path"}}}}}},
		{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: []acp.SessionConfigOption{option}}},
		{CurrentModeUpdate: &acp.SessionCurrentModeUpdate{CurrentModeId: "ask"}},
		{UsageUpdate: &acp.SessionUsageUpdate{Size: 10, Used: 2}},
	}
	for _, update := range updates {
		managed.emitUpdate(update)
	}
	seen := map[host.EventType]bool{}
	for len(events) > 0 {
		event := <-events
		seen[event.Type] = true
		if event.Type == host.EventToolStarted {
			if images, _ := event.Data["images"].([]map[string]any); len(images) != 1 || images[0]["data_base64"] != "image-data" {
				t.Fatalf("tool images = %#v", event.Data)
			}
		}
		if event.Type == host.EventAvailableCommands {
			commands, _ := event.Data["available_commands"].([]host.BackendSlashCommand)
			if len(commands) != 1 || commands[0].InputHint != "path" {
				t.Fatalf("available commands = %#v", event.Data)
			}
		}
	}
	for _, kind := range []host.EventType{host.EventMessage, host.EventThinking, host.EventToolStarted, host.EventToolOutput, host.EventTodoUpdate, host.EventAvailableCommands, host.EventConfigCatalog, host.EventModels, host.EventTraceUpdated} {
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
	waitForAdapterTurn(t, adapter, "config")
	if _, sendErr := adapter.SendTurn(context.Background(), "config", host.SendTurnRequest{
		Prompt: "two", Model: "model-b", Reasoning: "low", PermissionMode: "plan",
	}, nil); sendErr != nil {
		t.Fatal(sendErr)
	}
	waitForAdapterTurn(t, adapter, "config")
	if closeErr := adapter.CloseSession("config"); closeErr != nil {
		t.Fatal(closeErr)
	}
	// #nosec G304 -- trace is a test-owned path below t.TempDir.
	raw, readErr := os.ReadFile(trace)
	if readErr != nil {
		t.Fatal(readErr)
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
	waitForAdapterTurn(t, adapter, "optional")
	if err := adapter.CloseSession("optional"); err != nil {
		t.Fatal(err)
	}
	// #nosec G304 -- trace is a test-owned path below t.TempDir.
	raw, readErr := os.ReadFile(trace)
	if readErr != nil {
		t.Fatal(readErr)
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

func TestAdapterLoadPreferenceAndDeadProcessRestart(t *testing.T) {
	trace := filepath.Join(t.TempDir(), "recovery.trace")
	base := Config{
		Backend:          "stub",
		Command:          os.Args[0],
		Args:             []string{"-test.run=TestACPChild", "--"},
		Environment:      append(os.Environ(), "DURABLE_ACP_RECOVERY_CHILD=1", "DURABLE_ACP_RECOVERY_TRACE="+trace),
		LoadSessionFirst: true,
		RestartOnExit:    true,
	}
	adapter := New(base)
	if _, err := adapter.StartSession(context.Background(), "load-first", host.StartSessionRequest{Worktree: t.TempDir(), ResumeBackendSessionID: "provider-recovery"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := adapter.CloseSession("load-first"); err != nil {
		t.Fatal(err)
	}
	raw, readErr := os.ReadFile(trace) //nolint:gosec // The test owns the isolated trace path.
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got := strings.Fields(string(raw)); !reflect.DeepEqual(got, []string{"session/load"}) {
		t.Fatalf("load-first trace = %#v", got)
	}

	if removeErr := os.Remove(trace); removeErr != nil {
		t.Fatal(removeErr)
	}
	base.LoadSessionFirst = false
	adapter = New(base)
	if _, startErr := adapter.StartSession(context.Background(), "restart", host.StartSessionRequest{Worktree: t.TempDir()}, nil); startErr != nil {
		t.Fatal(startErr)
	}
	if _, sendErr := adapter.SendTurn(context.Background(), "restart", host.SendTurnRequest{Prompt: "exit"}, nil); sendErr != nil {
		t.Fatal(sendErr)
	}
	managed, sessionErr := adapter.session("restart")
	if sessionErr != nil {
		t.Fatal(sessionErr)
	}
	select {
	case <-managed.conn.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("recovery child did not exit")
	}
	if _, sendErr := adapter.SendTurn(context.Background(), "restart", host.SendTurnRequest{Prompt: "again"}, nil); sendErr != nil {
		t.Fatal(sendErr)
	}
	waitForAdapterTurn(t, adapter, "restart")
	if closeErr := adapter.CloseSession("restart"); closeErr != nil {
		t.Fatal(closeErr)
	}
	raw, readErr = os.ReadFile(trace) //nolint:gosec // The test owns the isolated trace path.
	if readErr != nil {
		t.Fatal(readErr)
	}
	got := strings.Fields(string(raw))
	want := []string{"session/new", "prompt:exit", "session/resume", "prompt:again"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restart trace = %#v, want %#v", got, want)
	}
}

func waitForAdapterTurn(t *testing.T, adapter *Adapter, sessionID string) {
	t.Helper()
	managed, err := adapter.session(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	managed.promptMu.Lock()
	turnID := managed.currentTurnID()
	managed.promptMu.Unlock()
	if turnID != "" {
		t.Fatalf("active turn after prompt completion = %q", turnID)
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
	if os.Getenv("DURABLE_ACP_RECOVERY_CHILD") == "1" {
		runRecoveryChild(t)
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

func runRecoveryChild(t *testing.T) {
	t.Helper()
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	trace := func(line string) {
		path := os.Getenv("DURABLE_ACP_RECOVERY_TRACE")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // The parent test owns the isolated trace path.
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
		case "session/new":
			trace("session/new")
			writeRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider-recovery", "configOptions": []any{}})
		case "session/resume", "session/load":
			trace(message.Method)
			writeRPC(t, encoder, message.ID, map[string]any{"sessionId": "provider-recovery", "configOptions": []any{}})
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
			trace("prompt:" + prompt)
			writeRPC(t, encoder, message.ID, map[string]any{"stopReason": "end_turn"})
			if prompt == "exit" {
				os.Exit(0)
			}
		case "provider/echo":
			var params map[string]any
			if err := json.Unmarshal(message.Params, &params); err != nil {
				t.Fatal(err)
			}
			writeRPC(t, encoder, message.ID, params)
		case "session/close":
			writeRPC(t, encoder, message.ID, map[string]any{})
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
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // The parent test owns the isolated trace path.
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
