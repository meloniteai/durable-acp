// Package antigravity provides the bundled Antigravity adapter.
package antigravity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"

	antigravityacp "github.com/shubzkothekar/antigravity-acp-go"
)

const Backend host.Backend = "antigravity"

type Config struct {
	Command          string
	ConversationsDir string
	StateDir         string
	Version          string
}

type Adapter struct {
	config         Config
	mu             sync.Mutex
	sessions       map[string]*managedSession
	newAgent       agentFactory
	discoverModels func(string) ([]string, error)
}

type managedSession struct {
	adapter   *Adapter
	agent     agent
	hostID    string
	backendID string
	worktree  string
	emit      host.EventSink

	turnMu     sync.Mutex
	turn       uint64
	turnID     string
	turnCancel context.CancelFunc

	interactionMu sync.Mutex
	interaction   uint64
	interactions  map[string]chan host.InteractionResponse
	done          chan struct{}
	doneOnce      sync.Once
}

type sessionClient struct {
	session *managedSession
}

type agent interface {
	NewSession(string, []string, antigravityacp.Client) (string, []antigravityacp.ConfigOption)
	ResumeSession(string, string, []string, antigravityacp.Client) ([]antigravityacp.ConfigOption, error)
	Prompt(string, any, antigravityacp.Client) (*antigravityacp.PromptOutcome, error)
	SetConfigOption(string, string, string) ([]antigravityacp.ConfigOption, error)
	Cancel(string)
	CloseSession(string)
}

type agentFactory func(command, conversationsDir, worktree, version, sessionsFile, stateDir string) agent

func New(configs ...Config) *Adapter {
	config := Config{Command: "agy", Version: "1.0.0"}
	if len(configs) > 0 {
		if strings.TrimSpace(configs[0].Command) != "" {
			config.Command = strings.TrimSpace(configs[0].Command)
		}
		if strings.TrimSpace(configs[0].Version) != "" {
			config.Version = strings.TrimSpace(configs[0].Version)
		}
		config.ConversationsDir = strings.TrimSpace(configs[0].ConversationsDir)
		config.StateDir = strings.TrimSpace(configs[0].StateDir)
	}
	return &Adapter{
		config: config, sessions: map[string]*managedSession{}, discoverModels: antigravityacp.DiscoverModels,
		newAgent: func(command, conversationsDir, worktree, version, sessionsFile, stateDir string) agent {
			store := antigravityacp.NewSessionStore(sessionsFile, stateDir)
			return antigravityacp.NewAgyAcpAgent(command, conversationsDir, worktree, false, version, store)
		},
	}
}

func (a *Adapter) Backend() host.Backend { return Backend }

func (a *Adapter) Detect(ctx context.Context) host.BackendStatus {
	resolved, err := acpx.Resolve(ctx, a.config.Command)
	if err != nil {
		return host.BackendStatus{Backend: Backend, Command: a.config.Command, Error: err.Error()}
	}
	return host.BackendStatus{Backend: Backend, Available: true, Command: resolved.Path}
}

func (a *Adapter) Catalog(ctx context.Context) (host.BackendCatalog, error) {
	resolved, err := acpx.Resolve(ctx, a.config.Command)
	if err != nil {
		return host.BackendCatalog{}, err
	}
	models, err := a.discoverModels(resolved.Path)
	if err != nil {
		return host.BackendCatalog{}, err
	}
	catalog := host.BackendCatalog{Models: make([]host.BackendModel, 0, len(models))}
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model != "" {
			catalog.Models = append(catalog.Models, host.BackendModel{ID: model, Label: model})
		}
	}
	return catalog, nil
}

func (a *Adapter) StartSession(ctx context.Context, sessionID string, request host.StartSessionRequest, emit host.EventSink) (host.BackendSession, error) {
	if strings.TrimSpace(sessionID) == "" {
		return host.BackendSession{}, errors.New("antigravity: session ID is required")
	}
	worktree, err := validateWorktree(request.Worktree)
	if err != nil {
		return host.BackendSession{}, err
	}
	provider, err := a.createAgent(ctx, worktree)
	if err != nil {
		return host.BackendSession{}, err
	}
	managed := &managedSession{
		adapter: a, agent: provider, hostID: sessionID, worktree: worktree, emit: emit,
		interactions: map[string]chan host.InteractionResponse{}, done: make(chan struct{}),
	}
	a.mu.Lock()
	if a.sessions[sessionID] != nil {
		a.mu.Unlock()
		return host.BackendSession{}, fmt.Errorf("antigravity: session %q already exists", sessionID)
	}
	a.sessions[sessionID] = managed
	a.mu.Unlock()

	client := &sessionClient{session: managed}
	backendID := strings.TrimSpace(request.ResumeBackendSessionID)
	var options []antigravityacp.ConfigOption
	if backendID == "" {
		backendID, options = provider.NewSession(worktree, nil, client)
	} else {
		options, err = provider.ResumeSession(backendID, worktree, nil, client)
		if err != nil {
			a.remove(sessionID, managed)
			return host.BackendSession{}, fmt.Errorf("antigravity: resume session: %w", err)
		}
	}
	managed.backendID = strings.TrimSpace(backendID)
	if managed.backendID == "" {
		a.remove(sessionID, managed)
		return host.BackendSession{}, errors.New("antigravity: provider returned an empty session ID")
	}
	options = managed.applyConfiguration(request.Model, request.PermissionMode, options)
	managed.emitConfig(options)
	state := host.BackendSession{ID: managed.backendID, ThreadID: managed.backendID}
	if strings.TrimSpace(request.Prompt) != "" {
		state, err = a.SendTurn(ctx, sessionID, host.SendTurnRequest{
			Prompt: request.Prompt, Attachments: request.Attachments, Model: request.Model, PermissionMode: request.PermissionMode,
		}, emit)
		if err != nil {
			_ = a.CloseSession(sessionID)
			return host.BackendSession{}, err
		}
	}
	return state, nil
}

func (a *Adapter) SendTurn(ctx context.Context, sessionID string, request host.SendTurnRequest, emit host.EventSink) (host.BackendSession, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return host.BackendSession{}, errors.New("antigravity: prompt is required")
	}
	managed.turnMu.Lock()
	if managed.turnID != "" {
		managed.turnMu.Unlock()
		return host.BackendSession{}, errors.New("antigravity: turn already active")
	}
	managed.turn++
	turnID := fmt.Sprintf("%s:%d", managed.backendID, managed.turn)
	turnContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	managed.turnID = turnID
	managed.turnCancel = cancel
	if emit != nil {
		managed.emit = emit
	}
	managed.turnMu.Unlock()
	managed.emitEvent(host.Event{Type: host.EventTurnStarted, BackendTurnID: turnID, Data: map[string]any{"turn_id": turnID}})
	go managed.runPrompt(turnContext, turnID, request)
	return host.BackendSession{ID: managed.backendID, ThreadID: managed.backendID, TurnID: turnID}, nil
}

func (s *managedSession) runPrompt(ctx context.Context, turnID string, request host.SendTurnRequest) {
	options := s.applyConfiguration(request.Model, request.PermissionMode, nil)
	if len(options) > 0 {
		s.emitConfig(options)
	}
	outcome, err := s.agent.Prompt(s.backendID, request.Prompt, &sessionClient{session: s})
	select {
	case <-ctx.Done():
		return
	default:
	}
	if !s.finishTurn(turnID) {
		return
	}
	if err != nil {
		s.emitEvent(host.Event{Type: host.EventTurnFailed, BackendTurnID: turnID, Message: err.Error()})
		return
	}
	if outcome != nil && strings.TrimSpace(outcome.Error) != "" {
		s.emitEvent(host.Event{Type: host.EventTurnFailed, BackendTurnID: turnID, Message: outcome.Error})
		return
	}
	stopReason := "end_turn"
	if outcome != nil && strings.TrimSpace(outcome.StopReason) != "" {
		stopReason = outcome.StopReason
	}
	s.emitEvent(host.Event{Type: host.EventTurnComplete, BackendTurnID: turnID, Data: map[string]any{"stop_reason": stopReason}})
}

func (a *Adapter) Interrupt(_ context.Context, sessionID string, _ host.EventSink) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	turnID, cancel := managed.takeTurn()
	if turnID == "" {
		return nil
	}
	managed.agent.Cancel(managed.backendID)
	if cancel != nil {
		cancel()
	}
	managed.cancelInteractions()
	managed.emitEvent(host.Event{Type: host.EventTurnFailed, BackendTurnID: turnID, Message: "antigravity turn interrupted", Data: map[string]any{"interrupted": true}})
	return nil
}

func (a *Adapter) CloseSession(sessionID string) error {
	a.mu.Lock()
	managed := a.sessions[sessionID]
	delete(a.sessions, sessionID)
	a.mu.Unlock()
	if managed == nil {
		return nil
	}
	managed.stop()
	managed.agent.CloseSession(managed.backendID)
	return nil
}

func (a *Adapter) RestartSession(ctx context.Context, sessionID string, _ host.EventSink) (host.BackendSession, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	managed.turnMu.Lock()
	active := managed.turnID != ""
	managed.turnMu.Unlock()
	if active {
		return host.BackendSession{}, errors.New("antigravity: cannot restart during an active turn")
	}
	provider, err := a.createAgent(ctx, managed.worktree)
	if err != nil {
		return host.BackendSession{}, err
	}
	options, err := provider.ResumeSession(managed.backendID, managed.worktree, nil, &sessionClient{session: managed})
	if err != nil {
		return host.BackendSession{}, fmt.Errorf("antigravity: restart session: %w", err)
	}
	managed.agent = provider
	managed.emitConfig(options)
	managed.emitEvent(host.Event{Type: host.EventAgentRecovered, Message: "Antigravity session recovered", Data: map[string]any{"restarted": true}})
	return host.BackendSession{ID: managed.backendID, ThreadID: managed.backendID}, nil
}

func (a *Adapter) RespondPermission(ctx context.Context, sessionID, requestID string, allow bool, message, _ string) error {
	action := "deny"
	if allow {
		action = "approve"
	}
	return a.RespondInteraction(ctx, sessionID, host.InteractionResponse{RequestID: requestID, Action: action, Message: message})
}

func (a *Adapter) RespondInteraction(_ context.Context, sessionID string, response host.InteractionResponse) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	managed.interactionMu.Lock()
	pending := managed.interactions[response.RequestID]
	if pending != nil {
		delete(managed.interactions, response.RequestID)
	}
	managed.interactionMu.Unlock()
	if pending == nil {
		return fmt.Errorf("antigravity: interaction %q not found", response.RequestID)
	}
	select {
	case pending <- response:
		managed.emitEvent(host.Event{Type: host.EventInteractionResolved, InteractionResponse: &response})
		return nil
	case <-managed.done:
		return errors.New("antigravity: session is closed")
	}
}

func (a *Adapter) createAgent(ctx context.Context, worktree string) (agent, error) {
	resolved, err := acpx.Resolve(ctx, a.config.Command)
	if err != nil {
		return nil, fmt.Errorf("antigravity: resolve command: %w", err)
	}
	conversationsDir, stateDir, err := a.directories()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("antigravity: create state directory: %w", err)
	}
	return a.newAgent(resolved.Path, conversationsDir, worktree, a.config.Version, filepath.Join(stateDir, "sessions.json"), stateDir), nil
}

func (a *Adapter) directories() (string, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", fmt.Errorf("antigravity: resolve user home: %w", err)
	}
	conversationsDir := a.config.ConversationsDir
	if conversationsDir == "" {
		conversationsDir = strings.TrimSpace(os.Getenv("AGY_CONVERSATIONS_DIR"))
	}
	if conversationsDir == "" {
		conversationsDir = filepath.Join(home, ".gemini", "antigravity-cli", "conversations")
	}
	stateDir := a.config.StateDir
	if stateDir == "" {
		stateDir = filepath.Join(home, ".agy-acp")
	}
	return conversationsDir, stateDir, nil
}

func (a *Adapter) session(sessionID string) (*managedSession, error) {
	a.mu.Lock()
	managed := a.sessions[sessionID]
	a.mu.Unlock()
	if managed == nil {
		return nil, fmt.Errorf("antigravity: session %q not found", sessionID)
	}
	return managed, nil
}

func (a *Adapter) remove(sessionID string, managed *managedSession) {
	a.mu.Lock()
	if a.sessions[sessionID] == managed {
		delete(a.sessions, sessionID)
	}
	a.mu.Unlock()
	managed.stop()
}

func (s *managedSession) applyConfiguration(model, mode string, fallback []antigravityacp.ConfigOption) []antigravityacp.ConfigOption {
	options := fallback
	for _, selection := range []struct{ id, value string }{{"model", strings.TrimSpace(model)}, {"mode", strings.TrimSpace(mode)}} {
		if selection.value == "" {
			continue
		}
		updated, err := s.agent.SetConfigOption(s.backendID, selection.id, selection.value)
		if err != nil {
			s.emitEvent(host.Event{Type: host.EventTraceUpdated, Message: err.Error(), Data: map[string]any{"configuration_error": err.Error()}})
			continue
		}
		options = updated
	}
	return options
}

func (s *managedSession) emitConfig(options []antigravityacp.ConfigOption) {
	catalog, model, mode := catalogFromOptions(options)
	s.emitEvent(host.Event{Type: host.EventConfigCatalog, Data: map[string]any{"catalog": catalog, "config_options": options, "current_model": model, "current_mode": mode}})
	if len(catalog.Models) > 0 {
		s.emitEvent(host.Event{Type: host.EventModels, Data: map[string]any{"models": catalog.Models, "current_model": model}})
	}
	if len(catalog.PermissionModes) > 0 {
		s.emitEvent(host.Event{Type: host.EventPermissionModes, Data: map[string]any{"permission_modes": catalog.PermissionModes, "current_mode": mode}})
	}
}

func catalogFromOptions(options []antigravityacp.ConfigOption) (host.BackendCatalog, string, string) {
	var catalog host.BackendCatalog
	var model, mode string
	for _, option := range options {
		category := strings.TrimSpace(option.Category)
		if category == "" {
			category = strings.TrimSpace(option.ID)
		}
		switch category {
		case "model":
			model = strings.TrimSpace(option.CurrentValue)
			for _, value := range option.Options {
				if id := strings.TrimSpace(value.Value); id != "" {
					catalog.Models = append(catalog.Models, host.BackendModel{ID: id, Label: value.Name})
				}
			}
		case "mode":
			mode = strings.TrimSpace(option.CurrentValue)
			for _, value := range option.Options {
				if id := strings.TrimSpace(value.Value); id != "" {
					catalog.PermissionModes = append(catalog.PermissionModes, host.BackendPermissionMode{ID: id, Label: value.Name, Description: value.Description})
				}
			}
		}
	}
	return catalog, model, mode
}

func (c *sessionClient) Update(sessionID string, update *antigravityacp.SessionUpdate) error {
	if update == nil {
		return nil
	}
	event := c.session.translateUpdate(update)
	if event.Type == "" {
		return nil
	}
	if event.BackendSessionID == "" {
		event.BackendSessionID = strings.TrimSpace(sessionID)
	}
	c.session.emitEvent(event)
	return nil
}

func (c *sessionClient) RequestPermission(params any) (any, error) {
	s := c.session
	s.interactionMu.Lock()
	s.interaction++
	requestID := fmt.Sprintf("%s:interaction:%d", s.backendID, s.interaction)
	response := make(chan host.InteractionResponse, 1)
	s.interactions[requestID] = response
	s.interactionMu.Unlock()
	data := map[string]any{"params": params}
	if values := valueMap(params); len(values) > 0 {
		toolCall := values
		if nested := mapValue(values, "toolCall"); len(nested) > 0 {
			toolCall = nested
		}
		data["tool_call"] = toolCall
	}
	request := host.InteractionRequest{ID: requestID, Kind: host.InteractionPermission, Title: "Permission requested", Message: fmt.Sprintf("Permission requested: %v", params), Data: data}
	s.emitEvent(host.Event{Type: host.EventInteractionRequested, Interaction: &request})
	timer := time.NewTimer(5 * time.Minute)
	defer timer.Stop()
	defer s.removeInteraction(requestID)
	select {
	case answer := <-response:
		return answer.Action == "approve" || answer.Action == "allow" || answer.Action == "accept", nil
	case <-s.done:
		return false, errors.New("antigravity: session closed")
	case <-timer.C:
		return false, errors.New("antigravity: permission request timed out")
	}
}

func (s *managedSession) translateUpdate(update *antigravityacp.SessionUpdate) host.Event {
	switch update.SessionUpdate {
	case "agent_message_chunk":
		text := contentText(update.Content)
		if text != "" {
			return host.Event{Type: host.EventMessage, Role: "assistant", Message: text, Data: map[string]any{"delta": text}}
		}
	case "agent_thought_chunk":
		text := contentText(update.Content)
		if text != "" {
			return host.Event{Type: host.EventThinking, Message: text, Data: map[string]any{"delta": text}}
		}
	case "tool_call", "tool_call_update":
		input := valueMap(update.RawInput)
		output := outputText(update.RawOutput)
		kind := acpx.NormalizeToolKind(update.Kind)
		command := acpx.ToolDisplayCommand(input)
		status := acpx.NormalizeToolStatus(update.Status)
		if status == "" {
			status = "in_progress"
			if update.SessionUpdate == "tool_call_update" {
				status = "completed"
			}
		}
		display := &host.ToolDisplay{ID: update.ToolCallID, Title: acpx.FirstNonEmpty(update.Title, acpx.DefaultToolTitle(kind, command)), Kind: kind, Status: status, Command: command, Target: acpx.ToolDisplayTarget(input)}
		eventType := host.EventToolStarted
		if update.SessionUpdate == "tool_call_update" || status == "completed" || status == "failed" {
			eventType = host.EventToolOutput
		}
		return host.Event{Type: eventType, Message: output, ToolDisplay: display, Data: map[string]any{"toolCallId": update.ToolCallID, "name": update.Kind, "input": input, "output": output, "status": status}}
	case "available_commands_update":
		commands := make([]host.BackendSlashCommand, 0, len(update.AvailableCommands))
		for _, command := range update.AvailableCommands {
			commands = append(commands, host.BackendSlashCommand{Name: command.Name, Description: command.Description})
		}
		return host.Event{Type: host.EventAvailableCommands, Data: map[string]any{"available_commands": commands}}
	case "config_option_update":
		catalog, model, mode := catalogFromOptions(update.ConfigOptions)
		return host.Event{Type: host.EventConfigCatalog, Data: map[string]any{"catalog": catalog, "config_options": update.ConfigOptions, "current_model": model, "current_mode": mode}}
	}
	return host.Event{}
}

func (s *managedSession) emitEvent(event host.Event) {
	if s.emit == nil {
		return
	}
	event.Backend = Backend
	if event.BackendSessionID == "" {
		event.BackendSessionID = s.backendID
	}
	if event.BackendThreadID == "" {
		event.BackendThreadID = event.BackendSessionID
	}
	if event.BackendTurnID == "" {
		event.BackendTurnID = s.currentTurnID()
	}
	s.emit(event)
}

func (s *managedSession) currentTurnID() string {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	return s.turnID
}

func (s *managedSession) finishTurn(turnID string) bool {
	s.turnMu.Lock()
	if turnID == "" || s.turnID != turnID {
		s.turnMu.Unlock()
		return false
	}
	cancel := s.turnCancel
	s.turnID = ""
	s.turnCancel = nil
	s.turnMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

func (s *managedSession) takeTurn() (string, context.CancelFunc) {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	turnID, cancel := s.turnID, s.turnCancel
	s.turnID = ""
	s.turnCancel = nil
	return turnID, cancel
}

func (s *managedSession) removeInteraction(requestID string) {
	s.interactionMu.Lock()
	delete(s.interactions, requestID)
	s.interactionMu.Unlock()
}

func (s *managedSession) cancelInteractions() {
	s.interactionMu.Lock()
	pending := make([]chan host.InteractionResponse, 0, len(s.interactions))
	for requestID, response := range s.interactions {
		delete(s.interactions, requestID)
		pending = append(pending, response)
	}
	s.interactionMu.Unlock()
	for _, response := range pending {
		select {
		case response <- host.InteractionResponse{Action: "cancel"}:
		default:
		}
	}
}

func (s *managedSession) stop() {
	s.doneOnce.Do(func() {
		close(s.done)
		_, cancel := s.takeTurn()
		if cancel != nil {
			cancel()
		}
		s.cancelInteractions()
	})
}

func validateWorktree(worktree string) (string, error) {
	worktree = strings.TrimSpace(worktree)
	if worktree == "" || !filepath.IsAbs(worktree) {
		return "", errors.New("antigravity: absolute worktree is required")
	}
	info, err := os.Stat(worktree)
	if err != nil {
		return "", fmt.Errorf("antigravity: inspect worktree: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("antigravity: worktree is not a directory")
	}
	return filepath.Clean(worktree), nil
}

func valueMap(value any) map[string]any {
	if value == nil {
		return nil
	}
	if mapped, ok := value.(map[string]any); ok {
		return mapped
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var mapped map[string]any
	if json.Unmarshal(raw, &mapped) != nil {
		return nil
	}
	return mapped
}

func mapValue(values map[string]any, key string) map[string]any {
	mapped, _ := values[key].(map[string]any)
	return mapped
}

func contentText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	mapped := valueMap(value)
	if mapped == nil || mapped["text"] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(mapped["text"]))
}

func outputText(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}
