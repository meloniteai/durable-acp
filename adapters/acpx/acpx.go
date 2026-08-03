// Package acpx provides the common ACP subprocess adapter used by the bundled
// provider packages. It is also suitable for a host's own standards-compliant
// ACP executable.
package acpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/client"
	"github.com/meloniteai/durable-acp/host"
)

// Config describes one ACP executable. Command is deliberately explicit: the
// embedding host chooses installation and upgrade policy rather than relying
// on an SDK-specific home directory or environment variable.
type Config struct {
	Backend     host.Backend
	Command     string
	Args        []string
	Environment []string
	Stderr      io.Writer
	ClientName  string
}

// Option customizes an ACP executable configuration.
type Option func(*Config)

// WithCommand replaces the provider's default ACP executable.
func WithCommand(command string) Option {
	return func(config *Config) { config.Command = strings.TrimSpace(command) }
}

// WithArgs replaces arguments passed to the ACP executable.
func WithArgs(args ...string) Option {
	return func(config *Config) { config.Args = append([]string(nil), args...) }
}

// WithEnvironment sets the full child environment. A nil environment inherits
// the host environment; an empty environment deliberately starts clean.
func WithEnvironment(environment []string) Option {
	return func(config *Config) { config.Environment = append([]string(nil), environment...) }
}

// WithStderr directs child diagnostics to a host-owned writer.
func WithStderr(writer io.Writer) Option {
	return func(config *Config) { config.Stderr = writer }
}

// WithClientName changes the neutral client implementation name advertised to
// ACP agents. It does not affect session persistence or provider behavior.
func WithClientName(name string) Option {
	return func(config *Config) { config.ClientName = strings.TrimSpace(name) }
}

// Adapter implements host.Adapter for a standard ACP command.
type Adapter struct {
	config Config

	mu       sync.Mutex
	sessions map[string]*managedSession
}

type managedSession struct {
	adapter   *Adapter
	hostID    string
	backendID string
	worktree  string
	emit      host.EventSink
	conn      *client.Connection
	model     string
	reasoning string
	mode      string

	promptMu sync.Mutex
	turn     atomic.Uint64

	interactionMu sync.Mutex
	interactions  map[string]chan host.InteractionResponse
	interactionID uint64
	done          chan struct{}
	doneOnce      sync.Once
}

// New creates an ACP command adapter. Invalid configuration is reported by
// StartSession and Detect so an application can still show all known backends.
func New(config Config, options ...Option) *Adapter {
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	config.Backend = host.Backend(strings.TrimSpace(string(config.Backend)))
	config.Command = strings.TrimSpace(config.Command)
	config.ClientName = strings.TrimSpace(config.ClientName)
	if config.ClientName == "" {
		config.ClientName = "durable-acp"
	}
	return &Adapter{config: config, sessions: map[string]*managedSession{}}
}

// Backend identifies the configured provider.
func (a *Adapter) Backend() host.Backend {
	if a == nil {
		return ""
	}
	return a.config.Backend
}

// Detect resolves the configured ACP executable without starting an agent.
func (a *Adapter) Detect(ctx context.Context) host.BackendStatus {
	status := host.BackendStatus{Backend: a.Backend(), Command: a.config.Command}
	resolved, err := Resolve(ctx, a.config.Command)
	if err != nil {
		status.Error = err.Error()
		return status
	}
	status.Available = true
	status.Command = resolved.Path
	return status
}

// StartSession launches one ACP subprocess and creates or resumes its agent
// session. Request Ext is intentionally opaque to this generic adapter.
func (a *Adapter) StartSession(ctx context.Context, sessionID string, request host.StartSessionRequest, emit host.EventSink) (host.BackendSession, error) {
	managed, state, err := a.openSession(ctx, sessionID, request, emit)
	if err != nil {
		return host.BackendSession{}, err
	}
	a.mu.Lock()
	if previous := a.sessions[sessionID]; previous != nil {
		a.mu.Unlock()
		managed.stop()
		_ = managed.conn.Close()
		return host.BackendSession{}, fmt.Errorf("acpx: session %q already started", sessionID)
	}
	a.sessions[sessionID] = managed
	a.mu.Unlock()

	if strings.TrimSpace(request.Prompt) != "" || len(request.Attachments) > 0 {
		turn, turnErr := a.SendTurn(ctx, sessionID, host.SendTurnRequest{
			Prompt:         request.Prompt,
			Attachments:    request.Attachments,
			Model:          request.Model,
			Reasoning:      request.Reasoning,
			PermissionMode: request.PermissionMode,
		}, nil)
		if turnErr != nil {
			_ = a.CloseSession(sessionID)
			return host.BackendSession{}, turnErr
		}
		state = turn
	}
	return state, nil
}

func (a *Adapter) openSession(ctx context.Context, sessionID string, request host.StartSessionRequest, emit host.EventSink) (*managedSession, host.BackendSession, error) {
	if a == nil || a.Backend() == "" {
		return nil, host.BackendSession{}, errors.New("acpx: backend is required")
	}
	if strings.TrimSpace(sessionID) == "" {
		return nil, host.BackendSession{}, errors.New("acpx: session ID is required")
	}
	if strings.TrimSpace(request.Worktree) == "" || !filepath.IsAbs(request.Worktree) {
		return nil, host.BackendSession{}, errors.New("acpx: absolute worktree is required")
	}
	if info, err := os.Stat(request.Worktree); err != nil || !info.IsDir() {
		if err == nil {
			err = errors.New("not a directory")
		}
		return nil, host.BackendSession{}, fmt.Errorf("acpx: inspect worktree: %w", err)
	}
	resolved, err := Resolve(ctx, a.config.Command)
	if err != nil {
		return nil, host.BackendSession{}, fmt.Errorf("acpx: resolve %s: %w", a.Backend(), err)
	}

	managed := &managedSession{
		adapter:      a,
		hostID:       sessionID,
		worktree:     filepath.Clean(request.Worktree),
		emit:         emit,
		model:        strings.TrimSpace(request.Model),
		reasoning:    strings.TrimSpace(request.Reasoning),
		mode:         strings.TrimSpace(request.PermissionMode),
		interactions: map[string]chan host.InteractionResponse{},
		done:         make(chan struct{}),
	}
	connection, err := client.Start(ctx, client.Spec{
		Command: resolved.Path,
		Args:    append([]string(nil), a.config.Args...),
		Dir:     managed.worktree,
		Env:     adapterEnvironment(a.config.Environment, resolved.PathEnv),
		Stderr:  a.config.Stderr,
		Handler: managed,
		Initialize: acp.InitializeRequest{
			ClientInfo: &acp.Implementation{Name: a.config.ClientName, Version: "1"},
			ClientCapabilities: acp.ClientCapabilities{
				Elicitation: &acp.ElicitationCapabilities{
					Form: &acp.ElicitationFormCapabilities{},
					Url:  &acp.ElicitationUrlCapabilities{},
				},
			},
		},
	})
	if err != nil {
		return nil, host.BackendSession{}, err
	}
	managed.conn = connection

	backendID := strings.TrimSpace(request.ResumeBackendSessionID)
	var options []acp.SessionConfigOption
	if backendID == "" {
		created, createErr := connection.NewSession(ctx, &acp.NewSessionRequest{Cwd: managed.worktree, McpServers: []acp.McpServer{}})
		if createErr != nil {
			_ = connection.Close()
			return nil, host.BackendSession{}, createErr
		}
		backendID = string(created.SessionId)
		managed.backendID = backendID
		options = created.ConfigOptions
		managed.emitConfig(created.ConfigOptions, created.Modes)
	} else {
		resumed, resumeErr := connection.ResumeSession(ctx, &acp.ResumeSessionRequest{Cwd: managed.worktree, SessionId: acp.SessionId(backendID)})
		if resumeErr != nil {
			loaded, loadErr := connection.LoadSession(ctx, &acp.LoadSessionRequest{Cwd: managed.worktree, SessionId: acp.SessionId(backendID)})
			if loadErr != nil {
				_ = connection.Close()
				return nil, host.BackendSession{}, fmt.Errorf("acpx: resume session: %w", errors.Join(resumeErr, loadErr))
			}
			managed.backendID = backendID
			options = loaded.ConfigOptions
			managed.emitConfig(loaded.ConfigOptions, loaded.Modes)
		} else {
			managed.backendID = backendID
			options = resumed.ConfigOptions
			managed.emitConfig(resumed.ConfigOptions, resumed.Modes)
		}
	}
	if err := managed.applySelections(ctx, options, request.Model, request.Reasoning, request.PermissionMode); err != nil {
		// Config selection is advisory across ACP implementations. The agent
		// session remains usable if it does not recognize one of the controls.
		managed.emitEvent(host.Event{Type: host.EventTraceUpdated, Message: err.Error(), Data: map[string]any{"source": "session_config"}})
	}
	state := host.BackendSession{ID: backendID, ThreadID: backendID}
	return managed, state, nil
}

// SendTurn sends an ACP prompt and emits its lifecycle around the synchronous
// ACP request. ACP session updates continue to stream through the same sink.
func (a *Adapter) SendTurn(ctx context.Context, sessionID string, request host.SendTurnRequest, _ host.EventSink) (host.BackendSession, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	blocks := promptBlocks(request)
	if len(blocks) == 0 {
		return host.BackendSession{}, errors.New("acpx: prompt or attachment is required")
	}
	managed.promptMu.Lock()
	defer managed.promptMu.Unlock()
	turnID := fmt.Sprintf("turn-%d", managed.turn.Add(1))
	managed.emitEvent(host.Event{Type: host.EventTurnStarted, BackendTurnID: turnID, Data: map[string]any{"turn_id": turnID}})
	response, err := managed.conn.Prompt(ctx, &acp.PromptRequest{SessionId: acp.SessionId(managed.backendID), Prompt: blocks})
	if err != nil {
		managed.emitEvent(host.Event{Type: host.EventTurnFailed, BackendTurnID: turnID, Message: err.Error()})
		return host.BackendSession{}, err
	}
	managed.emitEvent(host.Event{Type: host.EventTurnComplete, BackendTurnID: turnID, Data: map[string]any{"stop_reason": response.StopReason}})
	return host.BackendSession{ID: managed.backendID, ThreadID: managed.backendID, TurnID: turnID}, nil
}

// Interrupt cancels the active ACP prompt and resolves outstanding permission
// and elicitation callbacks with cancellation.
func (a *Adapter) Interrupt(ctx context.Context, sessionID string, _ host.EventSink) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	managed.cancelInteractions()
	return managed.conn.Cancel(ctx, &acp.CancelNotification{SessionId: acp.SessionId(managed.backendID)})
}

// RestartSession replaces the ACP subprocess and resumes its provider session.
func (a *Adapter) RestartSession(ctx context.Context, sessionID string, emit host.EventSink) (host.BackendSession, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	managed.promptMu.Lock()
	defer managed.promptMu.Unlock()
	managed.stop()
	_ = managed.conn.Close()
	replacement, state, err := a.openSession(ctx, sessionID, host.StartSessionRequest{
		Worktree:               managed.worktree,
		ResumeBackendSessionID: managed.backendID,
		Model:                  managed.model,
		Reasoning:              managed.reasoning,
		PermissionMode:         managed.mode,
	}, emit)
	if err != nil {
		return host.BackendSession{}, err
	}
	a.mu.Lock()
	if a.sessions[sessionID] != managed {
		a.mu.Unlock()
		replacement.stop()
		_ = replacement.conn.Close()
		return host.BackendSession{}, fmt.Errorf("acpx: session %q changed during restart", sessionID)
	}
	a.sessions[sessionID] = replacement
	a.mu.Unlock()
	replacement.emitEvent(host.Event{Type: host.EventAgentRecovered, Message: "ACP session restarted", Data: map[string]any{"restarted": true}})
	return state, nil
}

// CloseSession terminates the ACP subprocess. It does not ask the agent to
// delete the provider session, which keeps durable Engine sessions resumable.
func (a *Adapter) CloseSession(sessionID string) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	managed := a.sessions[sessionID]
	delete(a.sessions, sessionID)
	a.mu.Unlock()
	if managed == nil {
		return nil
	}
	managed.stop()
	return managed.conn.Close()
}

// RespondInteraction resolves a permission or elicitation currently awaiting
// input from the host UI.
func (a *Adapter) RespondInteraction(_ context.Context, sessionID string, response host.InteractionResponse) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(response.RequestID) == "" {
		return errors.New("acpx: interaction request ID is required")
	}
	managed.interactionMu.Lock()
	pending := managed.interactions[response.RequestID]
	if pending != nil {
		delete(managed.interactions, response.RequestID)
	}
	managed.interactionMu.Unlock()
	if pending == nil {
		return fmt.Errorf("acpx: interaction %q not found", response.RequestID)
	}
	select {
	case pending <- response:
		return nil
	case <-managed.done:
		return errors.New("acpx: session is closed")
	}
}

// Catalog creates a short-lived ACP session to discover advertised selection
// options. It is intentionally best-effort; Runtime preserves its last cache
// when a provider is unavailable.
func (a *Adapter) Catalog(ctx context.Context) (host.BackendCatalog, error) {
	if a == nil {
		return host.BackendCatalog{}, errors.New("acpx: nil adapter")
	}
	resolved, err := Resolve(ctx, a.config.Command)
	if err != nil {
		return host.BackendCatalog{}, err
	}
	directory, err := os.MkdirTemp("", "durable-acp-catalog-")
	if err != nil {
		return host.BackendCatalog{}, err
	}
	defer func() { _ = os.RemoveAll(directory) }()
	connection, err := client.Start(ctx, client.Spec{
		Command: resolved.Path,
		Args:    append([]string(nil), a.config.Args...),
		Dir:     directory,
		Env:     adapterEnvironment(a.config.Environment, resolved.PathEnv),
		Stderr:  a.config.Stderr,
		Initialize: acp.InitializeRequest{
			ClientInfo: &acp.Implementation{Name: a.config.ClientName, Version: "1"},
		},
	})
	if err != nil {
		return host.BackendCatalog{}, err
	}
	defer func() { _ = connection.Close() }()
	created, err := connection.NewSession(ctx, &acp.NewSessionRequest{Cwd: directory, McpServers: []acp.McpServer{}})
	if err != nil {
		return host.BackendCatalog{}, err
	}
	return catalogFromConfig(created.ConfigOptions, created.Modes), nil
}

// ResolveCommand returns only the executable path from Resolve. Callers that
// launch a child should prefer Resolve and pass its PathEnv to
// AgentEnvironment.
func ResolveCommand(command string) (string, error) {
	resolved, err := Resolve(context.Background(), command)
	return resolved.Path, err
}

func adapterEnvironment(configured []string, pathEnv string) []string {
	if configured != nil {
		return childEnvironment(configured)
	}
	return AgentEnvironment(pathEnv)
}

func (a *Adapter) session(id string) (*managedSession, error) {
	if a == nil {
		return nil, errors.New("acpx: nil adapter")
	}
	a.mu.Lock()
	managed := a.sessions[strings.TrimSpace(id)]
	a.mu.Unlock()
	if managed == nil {
		return nil, fmt.Errorf("acpx: session %q not found", id)
	}
	return managed, nil
}

func (s *managedSession) SessionUpdate(_ context.Context, notification *acp.SessionNotification) error {
	if notification == nil {
		return nil
	}
	s.emitUpdate(notification.Update)
	return nil
}

func (s *managedSession) RequestPermission(_ context.Context, request *acp.RequestPermissionRequest) (*acp.RequestPermissionResponse, error) {
	if request == nil {
		return nil, errors.New("acpx: nil permission request")
	}
	id, response := s.awaitInteraction(host.InteractionRequest{
		Kind:    host.InteractionPermission,
		Title:   permissionTitle(request.ToolCall),
		Options: permissionOptions(request.Options),
		Data: map[string]any{
			"tool_call": valueMap(request.ToolCall),
		},
	})
	defer s.removeInteraction(id)
	select {
	case answer := <-response:
		optionID := choosePermissionOption(answer, request.Options)
		if optionID == "" {
			return &acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}, nil
		}
		return &acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(optionID)}}}, nil
	case <-s.done:
		return &acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}, nil
	}
}

func (s *managedSession) CreateElicitation(_ context.Context, request *acp.CreateElicitationRequest) (*acp.CreateElicitationResponse, error) {
	if request == nil {
		return nil, errors.New("acpx: nil elicitation request")
	}
	kind, title, message := elicitationDetails(request)
	id, response := s.awaitInteraction(host.InteractionRequest{
		Kind:    kind,
		Title:   title,
		Message: message,
		Data:    map[string]any{"elicitation": valueMap(request)},
	})
	defer s.removeInteraction(id)
	select {
	case answer := <-response:
		switch strings.ToLower(strings.TrimSpace(answer.Action)) {
		case "submit", "accept", "approve":
			return &acp.CreateElicitationResponse{Accept: &acp.CreateElicitationAccept{Content: answer.Values}}, nil
		case "decline", "deny":
			return &acp.CreateElicitationResponse{Decline: &acp.CreateElicitationDecline{}}, nil
		default:
			return &acp.CreateElicitationResponse{Cancel: &acp.CreateElicitationCancel{}}, nil
		}
	case <-s.done:
		return &acp.CreateElicitationResponse{Cancel: &acp.CreateElicitationCancel{}}, nil
	}
}

func (s *managedSession) awaitInteraction(request host.InteractionRequest) (string, <-chan host.InteractionResponse) {
	s.interactionMu.Lock()
	s.interactionID++
	id := fmt.Sprintf("interaction-%d", s.interactionID)
	request.ID = id
	response := make(chan host.InteractionResponse, 1)
	s.interactions[id] = response
	s.interactionMu.Unlock()
	s.emitEvent(host.Event{Type: host.EventInteractionRequested, Interaction: &request})
	return id, response
}

func (s *managedSession) removeInteraction(id string) {
	s.interactionMu.Lock()
	delete(s.interactions, id)
	s.interactionMu.Unlock()
}

func (s *managedSession) cancelInteractions() {
	s.interactionMu.Lock()
	responses := make([]chan host.InteractionResponse, 0, len(s.interactions))
	for id, response := range s.interactions {
		delete(s.interactions, id)
		responses = append(responses, response)
	}
	s.interactionMu.Unlock()
	for _, response := range responses {
		select {
		case response <- host.InteractionResponse{Action: "cancel"}:
		default:
		}
	}
}

func (s *managedSession) stop() {
	s.doneOnce.Do(func() {
		close(s.done)
		s.cancelInteractions()
	})
}

func (s *managedSession) emitEvent(event host.Event) {
	if s.emit == nil {
		return
	}
	event.Backend = s.adapter.Backend()
	if event.BackendSessionID == "" {
		event.BackendSessionID = s.backendID
	}
	if event.BackendThreadID == "" {
		event.BackendThreadID = s.backendID
	}
	s.emit(event)
}

func (s *managedSession) emitUpdate(update acp.SessionUpdate) {
	switch {
	case update.UserMessageChunk != nil:
		s.emitEvent(host.Event{Type: host.EventMessage, Role: "user", Message: contentText(update.UserMessageChunk.Content)})
	case update.AgentMessageChunk != nil:
		s.emitEvent(host.Event{Type: host.EventMessage, Role: "assistant", Message: contentText(update.AgentMessageChunk.Content)})
	case update.AgentThoughtChunk != nil:
		s.emitEvent(host.Event{Type: host.EventThinking, Message: contentText(update.AgentThoughtChunk.Content)})
	case update.ToolCall != nil:
		s.emitEvent(host.Event{Type: host.EventToolStarted, ToolDisplay: toolDisplay(update.ToolCall.ToolCallId, update.ToolCall.Title, update.ToolCall.Kind, update.ToolCall.Status), Data: valueMap(*update.ToolCall)})
	case update.ToolCallUpdate != nil:
		status := acp.ToolCallStatus("")
		if update.ToolCallUpdate.Status != nil {
			status = *update.ToolCallUpdate.Status
		}
		var title string
		if update.ToolCallUpdate.Title != nil {
			title = *update.ToolCallUpdate.Title
		}
		var kind acp.ToolKind
		if update.ToolCallUpdate.Kind != nil {
			kind = *update.ToolCallUpdate.Kind
		}
		s.emitEvent(host.Event{Type: host.EventToolOutput, ToolDisplay: toolDisplay(update.ToolCallUpdate.ToolCallId, title, kind, status), Data: valueMap(*update.ToolCallUpdate)})
	case update.Plan != nil:
		s.emitEvent(host.Event{Type: host.EventPlanUpdate, Data: valueMap(*update.Plan)})
	case update.AvailableCommandsUpdate != nil:
		commands := make([]host.BackendSlashCommand, 0, len(update.AvailableCommandsUpdate.AvailableCommands))
		for _, command := range update.AvailableCommandsUpdate.AvailableCommands {
			commands = append(commands, host.BackendSlashCommand{Name: command.Name, Description: command.Description})
		}
		s.emitEvent(host.Event{Type: host.EventAvailableCommands, Data: map[string]any{"available_commands": commands}})
	case update.ConfigOptionUpdate != nil:
		s.emitConfig(update.ConfigOptionUpdate.ConfigOptions, nil)
	case update.CurrentModeUpdate != nil:
		s.emitEvent(host.Event{Type: host.EventConfigCatalog, Data: map[string]any{"current_mode": update.CurrentModeUpdate.CurrentModeId}})
	case update.UsageUpdate != nil:
		s.emitEvent(host.Event{Type: host.EventTraceUpdated, Data: valueMap(*update.UsageUpdate)})
	}
}

func (s *managedSession) emitConfig(options []acp.SessionConfigOption, modes *acp.SessionModeState) {
	catalog := catalogFromConfig(options, modes)
	data := map[string]any{"catalog": catalog, "config_options": valueMap(options)}
	s.emitEvent(host.Event{Type: host.EventConfigCatalog, Data: data})
	if len(catalog.Models) > 0 {
		s.emitEvent(host.Event{Type: host.EventModels, Data: map[string]any{"models": catalog.Models}})
	}
	if len(catalog.PermissionModes) > 0 {
		s.emitEvent(host.Event{Type: host.EventPermissionModes, Data: map[string]any{"permission_modes": catalog.PermissionModes}})
	}
	if len(catalog.Reasoning) > 0 {
		s.emitEvent(host.Event{Type: host.EventReasoningLevels, Data: map[string]any{"reasoning": catalog.Reasoning}})
	}
}

func (s *managedSession) applySelections(ctx context.Context, options []acp.SessionConfigOption, model, reasoning, mode string) error {
	selections := []struct {
		value string
		keys  []string
	}{
		{value: strings.TrimSpace(model), keys: []string{"model"}},
		{value: strings.TrimSpace(reasoning), keys: []string{"thought_level", "reasoning", "reasoning_effort", "effort"}},
		{value: strings.TrimSpace(mode), keys: []string{"mode", "permission_mode"}},
	}
	var joined error
	for _, selection := range selections {
		if selection.value == "" {
			continue
		}
		optionID := findOption(options, selection.keys...)
		if optionID == "" {
			continue
		}
		_, err := s.conn.SetSessionConfigOption(ctx, &acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
			ConfigId:  acp.SessionConfigId(optionID),
			SessionId: acp.SessionId(s.backendID),
			Value:     acp.SessionConfigValueId(selection.value),
		}})
		joined = errors.Join(joined, err)
	}
	return joined
}

func promptBlocks(request host.SendTurnRequest) []acp.ContentBlock {
	blocks := make([]acp.ContentBlock, 0, 1+len(request.Attachments))
	if text := strings.TrimSpace(request.Prompt); text != "" {
		blocks = append(blocks, acp.TextBlock(text))
	}
	for _, attachment := range request.Attachments {
		if strings.HasPrefix(strings.ToLower(attachment.MimeType), "image/") && attachment.DataBase64 != "" {
			blocks = append(blocks, acp.ImageBlock(attachment.DataBase64, attachment.MimeType))
			continue
		}
		if path := strings.TrimSpace(attachment.Path); path != "" {
			name := strings.TrimSpace(attachment.Name)
			if name == "" {
				name = filepath.Base(path)
			}
			blocks = append(blocks, acp.ResourceLinkBlock(name, "file://"+path))
		}
	}
	return blocks
}

func childEnvironment(environment []string) []string {
	if environment == nil {
		return nil
	}
	return append([]string(nil), environment...)
}

func contentText(block acp.ContentBlock) string {
	switch {
	case block.Text != nil:
		return block.Text.Text
	case block.ResourceLink != nil:
		return block.ResourceLink.Uri
	case block.Image != nil:
		return "[image]"
	case block.Audio != nil:
		return "[audio]"
	case block.Resource != nil:
		return "[resource]"
	default:
		return ""
	}
}

func toolDisplay(id acp.ToolCallId, title string, kind acp.ToolKind, status acp.ToolCallStatus) *host.ToolDisplay {
	if id == "" && title == "" && kind == "" && status == "" {
		return nil
	}
	return &host.ToolDisplay{ID: string(id), Title: title, Kind: string(kind), Status: string(status)}
}

func permissionTitle(tool acp.ToolCallUpdate) string {
	if tool.Title != nil && strings.TrimSpace(*tool.Title) != "" {
		return *tool.Title
	}
	return "Permission requested"
}

func permissionOptions(options []acp.PermissionOption) []host.InteractionOption {
	result := make([]host.InteractionOption, 0, len(options))
	for _, option := range options {
		result = append(result, host.InteractionOption{ID: string(option.OptionId), Label: option.Name, Description: string(option.Kind)})
	}
	return result
}

func choosePermissionOption(response host.InteractionResponse, options []acp.PermissionOption) string {
	if id := strings.TrimSpace(response.OptionID); id != "" {
		for _, option := range options {
			if id == string(option.OptionId) {
				return id
			}
		}
		return ""
	}
	wantAllow := response.Action == "approve" || response.Action == "allow" || response.Action == "accept"
	for _, option := range options {
		allow := option.Kind == acp.PermissionOptionKindAllowOnce || option.Kind == acp.PermissionOptionKindAllowAlways
		if allow == wantAllow {
			return string(option.OptionId)
		}
	}
	return ""
}

func elicitationDetails(request *acp.CreateElicitationRequest) (host.InteractionKind, string, string) {
	switch {
	case request.Form != nil:
		return host.InteractionForm, "Input requested", request.Form.Message
	case request.Url != nil:
		return host.InteractionChoice, "Continue in browser", request.Url.Message
	case request.Other != nil:
		return host.InteractionForm, "Input requested", request.Other.Message
	default:
		return host.InteractionForm, "Input requested", ""
	}
}

func findOption(options []acp.SessionConfigOption, keys ...string) string {
	for _, option := range options {
		if option.Select == nil {
			continue
		}
		id := string(option.Select.Id)
		category := ""
		if option.Select.Category != nil {
			category = string(*option.Select.Category)
		}
		for _, key := range keys {
			if id == key || category == key {
				return id
			}
		}
	}
	return ""
}

func catalogFromConfig(options []acp.SessionConfigOption, modes *acp.SessionModeState) host.BackendCatalog {
	catalog := host.BackendCatalog{}
	for _, option := range options {
		if option.Select == nil {
			continue
		}
		category := ""
		if option.Select.Category != nil {
			category = string(*option.Select.Category)
		}
		id := string(option.Select.Id)
		for _, value := range selectOptions(option.Select.Options) {
			item := host.InteractionOption{ID: string(value.Value), Label: value.Name}
			switch {
			case category == "model" || id == "model":
				catalog.Models = append(catalog.Models, host.BackendModel{ID: item.ID, Label: item.Label})
			case category == "mode" || id == "mode" || id == "permission_mode":
				catalog.PermissionModes = append(catalog.PermissionModes, host.BackendPermissionMode{ID: item.ID, Label: item.Label})
			case category == "thought_level" || id == "reasoning" || id == "reasoning_effort" || id == "effort":
				catalog.Reasoning = append(catalog.Reasoning, host.BackendReasoning{ID: item.ID, Label: item.Label})
			}
		}
	}
	if modes != nil {
		for _, mode := range modes.AvailableModes {
			catalog.PermissionModes = append(catalog.PermissionModes, host.BackendPermissionMode{ID: string(mode.Id), Label: mode.Name})
		}
	}
	return catalog
}

func selectOptions(options acp.SessionConfigSelectOptions) []acp.SessionConfigSelectOption {
	if options.Ungrouped != nil {
		return append([]acp.SessionConfigSelectOption(nil), (*options.Ungrouped)...)
	}
	if options.Grouped == nil {
		return nil
	}
	result := make([]acp.SessionConfigSelectOption, 0)
	for _, group := range *options.Grouped {
		result = append(result, group.Options...)
	}
	return result
}

func valueMap(value any) map[string]any {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var result map[string]any
	if json.Unmarshal(raw, &result) != nil {
		return nil
	}
	return result
}
