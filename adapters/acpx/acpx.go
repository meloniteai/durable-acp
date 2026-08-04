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
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/client"
	"github.com/meloniteai/durable-acp/host"
)

// Config describes one ACP executable. Command is deliberately explicit: the
// embedding host chooses installation and upgrade policy rather than relying
// on an SDK-specific home directory or environment variable.
type Config struct {
	Backend                 host.Backend
	Command                 string
	Args                    []string
	Environment             []string
	Stderr                  io.Writer
	ClientName              string
	ClientTitle             string
	ClientVersion           string
	ClientCapabilities      *acp.ClientCapabilities
	InitializeFields        map[string]any
	ClientCapabilityFields  map[string]any
	LoadSessionFirst        bool
	RestartOnExit           bool
	LegacyExtensions        bool
	BestEffortConfiguration bool
	SessionModeValues       []string
	DoneCompletionGrace     time.Duration
	CompleteOnDone          bool
	ModelInPrompt           bool
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

func WithClientInfo(name, title, version string) Option {
	return func(config *Config) {
		config.ClientName = strings.TrimSpace(name)
		config.ClientTitle = strings.TrimSpace(title)
		config.ClientVersion = strings.TrimSpace(version)
	}
}

func WithClientCapabilities(capabilities acp.ClientCapabilities) Option {
	return func(config *Config) { config.ClientCapabilities = &capabilities }
}

func WithLoadSessionFirst(enabled bool) Option {
	return func(config *Config) { config.LoadSessionFirst = enabled }
}

func WithRestartOnExit(enabled bool) Option {
	return func(config *Config) { config.RestartOnExit = enabled }
}

func WithLegacyExtensions(enabled bool) Option {
	return func(config *Config) { config.LegacyExtensions = enabled }
}

func WithBestEffortConfiguration(enabled bool) Option {
	return func(config *Config) { config.BestEffortConfiguration = enabled }
}

func WithSessionModeValues(values ...string) Option {
	return func(config *Config) { config.SessionModeValues = append([]string(nil), values...) }
}

func WithDoneCompletionGrace(grace time.Duration) Option {
	return func(config *Config) {
		config.DoneCompletionGrace = grace
		config.CompleteOnDone = true
	}
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
	startupMu sync.Mutex
	startup   []acp.SessionNotification
	model     string
	reasoning string
	mode      string
	configMu  sync.RWMutex
	options   []acp.SessionConfigOption
	modes     *acp.SessionModeState

	promptMu   sync.Mutex
	turn       atomic.Uint64
	turnMu     sync.RWMutex
	turnID     string
	turnCancel context.CancelFunc
	toolMu     sync.Mutex
	tools      map[string]toolState
	toolActive bool

	interactionMu sync.Mutex
	interactions  map[string]*pendingInteraction
	interactionID uint64
	done          chan struct{}
	doneOnce      sync.Once
	replayMu      sync.Mutex
	replaying     bool
	replayFirst   bool
	forkMu        sync.Mutex
	forks         map[string]*forkInteractionPolicy
}

type pendingInteraction struct {
	request  host.InteractionRequest
	response chan host.InteractionResponse
}

type toolState struct {
	kind   string
	target string
}

type forkInteractionPolicy struct {
	allowedToolPrefixes []string
	tools               map[string]string
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
	if config.ClientVersion == "" {
		config.ClientVersion = "1"
	}
	config.InitializeFields = maps.Clone(config.InitializeFields)
	config.ClientCapabilityFields = maps.Clone(config.ClientCapabilityFields)
	return &Adapter{config: config, sessions: map[string]*managedSession{}}
}

func adapterInitialize(config Config) acp.InitializeRequest {
	capabilities := acp.ClientCapabilities{
		Elicitation: &acp.ElicitationCapabilities{
			Form: &acp.ElicitationFormCapabilities{},
			Url:  &acp.ElicitationUrlCapabilities{},
		},
	}
	if config.ClientCapabilities != nil {
		capabilities = *config.ClientCapabilities
	}
	var title *string
	if config.ClientTitle != "" {
		value := config.ClientTitle
		title = &value
	}
	return acp.InitializeRequest{
		ClientInfo: &acp.Implementation{
			Name:    config.ClientName,
			Title:   title,
			Version: config.ClientVersion,
		},
		ClientCapabilities: capabilities,
	}
}

func resumeSession(ctx context.Context, connection *client.Connection, worktree, backendID string, loadFirst bool) ([]acp.SessionConfigOption, *acp.SessionModeState, error) {
	load := func() ([]acp.SessionConfigOption, *acp.SessionModeState, error) {
		response, err := connection.LoadSession(ctx, &acp.LoadSessionRequest{Cwd: worktree, SessionId: acp.SessionId(backendID), McpServers: []acp.McpServer{}})
		if err != nil {
			return nil, nil, err
		}
		return response.ConfigOptions, response.Modes, nil
	}
	resume := func() ([]acp.SessionConfigOption, *acp.SessionModeState, error) {
		response, err := connection.ResumeSession(ctx, &acp.ResumeSessionRequest{Cwd: worktree, SessionId: acp.SessionId(backendID), McpServers: []acp.McpServer{}})
		if err != nil {
			return nil, nil, err
		}
		return response.ConfigOptions, response.Modes, nil
	}
	first, second := resume, load
	label := "resume"
	if loadFirst {
		first, second = load, resume
		label = "load"
	}
	options, modes, firstErr := first()
	if firstErr == nil {
		return options, modes, nil
	}
	options, modes, secondErr := second()
	if secondErr == nil {
		return options, modes, nil
	}
	return nil, nil, fmt.Errorf("acpx: %s session: %w", label, errors.Join(firstErr, secondErr))
}

func connectionDone(connection *client.Connection) bool {
	if connection == nil {
		return true
	}
	select {
	case <-connection.Done():
		return true
	default:
		return false
	}
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
	managed, state, err := a.openSession(ctx, sessionID, request, emit, true, a.config.LoadSessionFirst)
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

func (a *Adapter) openSession(ctx context.Context, sessionID string, request host.StartSessionRequest, emit host.EventSink, forceSelections, loadFirst bool) (*managedSession, host.BackendSession, error) {
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
		interactions: map[string]*pendingInteraction{},
		tools:        map[string]toolState{},
		forks:        map[string]*forkInteractionPolicy{},
		done:         make(chan struct{}),
	}
	connection, err := client.Start(ctx, client.Spec{
		Command:                resolved.Path,
		Args:                   append([]string(nil), a.config.Args...),
		Dir:                    managed.worktree,
		Env:                    adapterEnvironment(a.config.Environment, resolved.PathEnv),
		Stderr:                 a.config.Stderr,
		Handler:                managed,
		Observe:                managed.observe,
		LegacyExtensions:       a.config.LegacyExtensions,
		Initialize:             adapterInitialize(a.config),
		InitializeFields:       a.config.InitializeFields,
		ClientCapabilityFields: a.config.ClientCapabilityFields,
	})
	if err != nil {
		return nil, host.BackendSession{}, err
	}
	managed.conn = connection

	backendID := strings.TrimSpace(request.ResumeBackendSessionID)
	var options []acp.SessionConfigOption
	var modes *acp.SessionModeState
	if backendID == "" {
		created, createErr := connection.NewSession(ctx, &acp.NewSessionRequest{Cwd: managed.worktree, McpServers: []acp.McpServer{}})
		if createErr != nil {
			_ = connection.Close()
			return nil, host.BackendSession{}, createErr
		}
		backendID = string(created.SessionId)
		managed.setBackendID(ctx, backendID)
		options = created.ConfigOptions
		modes = created.Modes
		managed.initializeSelections(options, modes)
		managed.emitConfig(created.ConfigOptions, created.Modes)
	} else {
		managed.setBackendID(ctx, backendID)
		managed.beginReplay()
		options, modes, err = resumeSession(ctx, connection, managed.worktree, backendID, loadFirst)
		managed.endReplay()
		if err != nil {
			_ = connection.Close()
			return nil, host.BackendSession{}, err
		}
		managed.initializeSelections(options, modes)
		managed.emitConfig(options, modes)
	}
	if forceSelections && strings.TrimSpace(request.Model) != "" {
		managed.setSelected("model", "")
	}
	if forceSelections && strings.TrimSpace(request.Reasoning) != "" {
		managed.setSelected("reasoning", "")
	}
	if forceSelections && strings.TrimSpace(request.PermissionMode) != "" {
		managed.setSelected("mode", "")
	}
	if err := managed.applySelections(ctx, request.Model, request.Reasoning, request.PermissionMode); err != nil {
		managed.stop()
		_ = connection.Close()
		return nil, host.BackendSession{}, err
	}
	state := host.BackendSession{ID: backendID, ThreadID: backendID}
	return managed, state, nil
}

// SendTurn starts an ACP prompt and streams its lifecycle through the session sink.
func (a *Adapter) SendTurn(ctx context.Context, sessionID string, request host.SendTurnRequest, _ host.EventSink) (host.BackendSession, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	blocks := promptBlocks(request)
	if len(blocks) == 0 {
		return host.BackendSession{}, errors.New("acpx: prompt or attachment is required")
	}
	if a.config.RestartOnExit && connectionDone(managed.conn) {
		if _, restartErr := a.RestartSession(ctx, sessionID, managed.emit); restartErr != nil {
			return host.BackendSession{}, restartErr
		}
		managed, err = a.session(sessionID)
		if err != nil {
			return host.BackendSession{}, err
		}
	}
	if !managed.promptMu.TryLock() {
		return host.BackendSession{}, fmt.Errorf("acpx: session %q already has an active turn", sessionID)
	}
	if configErr := managed.applySelections(ctx, request.Model, request.Reasoning, request.PermissionMode); configErr != nil {
		managed.promptMu.Unlock()
		return host.BackendSession{}, configErr
	}
	turnID := fmt.Sprintf("%s:%d", managed.backendID, managed.turn.Add(1))
	turnContext, cancelTurn := context.WithCancel(context.WithoutCancel(ctx))
	managed.setTurn(turnID, cancelTurn)
	managed.emitEvent(host.Event{Type: host.EventTurnStarted, BackendTurnID: turnID, Data: map[string]any{"turn_id": turnID}})
	promptFields := map[string]any{}
	if a.config.ModelInPrompt && strings.TrimSpace(request.Model) != "" {
		promptFields["model"] = strings.TrimSpace(request.Model)
	}
	go managed.runPrompt(turnContext, turnID, blocks, promptFields)
	return host.BackendSession{ID: managed.backendID, ThreadID: managed.backendID, TurnID: turnID}, nil
}

func (s *managedSession) runPrompt(ctx context.Context, turnID string, blocks []acp.ContentBlock, fields map[string]any) {
	defer s.promptMu.Unlock()
	defer s.clearTools()
	target := s
	response, err := s.conn.PromptWithFields(ctx, &acp.PromptRequest{SessionId: acp.SessionId(s.backendID), Prompt: blocks}, fields)
	if err != nil && s.adapter.config.RestartOnExit && connectionDone(s.conn) {
		replacementContext := context.WithoutCancel(ctx)
		replacement, _, restartErr := s.adapter.replaceSession(replacementContext, s.hostID, s, s.emit)
		if restartErr == nil {
			replacement.turn.Store(s.turn.Load())
			retryContext, cancelRetry := context.WithCancel(replacementContext)
			replacement.setTurn(turnID, cancelRetry)
			replacement.promptMu.Lock()
			response, err = replacement.conn.PromptWithFields(retryContext, &acp.PromptRequest{SessionId: acp.SessionId(replacement.backendID), Prompt: blocks}, fields)
			replacement.promptMu.Unlock()
			target = replacement
		} else {
			err = errors.Join(err, restartErr)
		}
	}
	if err != nil {
		if target.finishTurn(turnID) {
			data := map[string]any{}
			if errors.Is(err, context.Canceled) {
				data["interrupted"] = true
			}
			target.emitEvent(host.Event{Type: host.EventTurnFailed, BackendTurnID: turnID, Message: err.Error(), Data: data})
		}
		return
	}
	if target.finishTurn(turnID) {
		if response.StopReason == acp.StopReasonCancelled {
			target.emitEvent(host.Event{Type: host.EventTurnFailed, BackendTurnID: turnID, Message: "ACP turn interrupted", Data: map[string]any{"interrupted": true, "stop_reason": response.StopReason}})
			return
		}
		target.emitEvent(host.Event{Type: host.EventTurnComplete, BackendTurnID: turnID, Data: map[string]any{"stop_reason": response.StopReason}})
	}
}

// Interrupt cancels the active ACP prompt and resolves outstanding permission
// and elicitation callbacks with cancellation.
func (a *Adapter) Interrupt(ctx context.Context, sessionID string, _ host.EventSink) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	if err := managed.conn.Cancel(ctx, &acp.CancelNotification{SessionId: acp.SessionId(managed.backendID)}); err != nil {
		return err
	}
	managed.cancelInteractions()
	turnID, cancel := managed.takeTurn()
	if cancel != nil {
		cancel()
	}
	if turnID != "" {
		managed.emitEvent(host.Event{Type: host.EventTurnFailed, BackendTurnID: turnID, Message: "ACP turn interrupted", Data: map[string]any{"interrupted": true}})
	}
	return nil
}

// RestartSession replaces the ACP subprocess and resumes its provider session.
func (a *Adapter) RestartSession(ctx context.Context, sessionID string, emit host.EventSink) (host.BackendSession, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	managed.promptMu.Lock()
	defer managed.promptMu.Unlock()
	_, state, err := a.replaceSession(ctx, sessionID, managed, emit)
	return state, err
}

func (a *Adapter) replaceSession(ctx context.Context, sessionID string, managed *managedSession, emit host.EventSink) (*managedSession, host.BackendSession, error) {
	managed.stop()
	_ = managed.conn.Close()
	replacement, state, err := a.openSession(ctx, sessionID, host.StartSessionRequest{
		Worktree:               managed.worktree,
		ResumeBackendSessionID: managed.backendID,
		Model:                  managed.selected("model"),
		Reasoning:              managed.selected("reasoning"),
		PermissionMode:         managed.selected("mode"),
	}, emit, false, false)
	if err != nil {
		return nil, host.BackendSession{}, err
	}
	replacement.turn.Store(managed.turn.Load())
	a.mu.Lock()
	if a.sessions[sessionID] != managed {
		a.mu.Unlock()
		replacement.stop()
		_ = replacement.conn.Close()
		return nil, host.BackendSession{}, fmt.Errorf("acpx: session %q changed during restart", sessionID)
	}
	a.sessions[sessionID] = replacement
	a.mu.Unlock()
	replacement.emitEvent(host.Event{Type: host.EventAgentRecovered, Message: "ACP session restarted", Data: map[string]any{"restarted": true}})
	return replacement, state, nil
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
	case pending.response <- response:
		managed.emitEvent(host.Event{Type: host.EventInteractionResolved, InteractionResponse: &response, Data: map[string]any{"request_id": response.RequestID, "action": response.Action}})
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
	collector := &catalogCollector{}
	connection, err := client.Start(ctx, client.Spec{
		Command:                resolved.Path,
		Args:                   append([]string(nil), a.config.Args...),
		Dir:                    directory,
		Env:                    adapterEnvironment(a.config.Environment, resolved.PathEnv),
		Stderr:                 a.config.Stderr,
		Handler:                collector,
		LegacyExtensions:       a.config.LegacyExtensions,
		Initialize:             adapterInitialize(a.config),
		InitializeFields:       a.config.InitializeFields,
		ClientCapabilityFields: a.config.ClientCapabilityFields,
	})
	if err != nil {
		return host.BackendCatalog{}, err
	}
	defer func() { _ = connection.Close() }()
	created, err := connection.NewSession(ctx, &acp.NewSessionRequest{Cwd: directory, McpServers: []acp.McpServer{}})
	if err != nil {
		return host.BackendCatalog{}, err
	}
	catalog := catalogFromConfig(created.ConfigOptions, created.Modes)
	modelOptionID := findOption(created.ConfigOptions, "model")
	if modelOptionID != "" {
		for index := range catalog.Models {
			response, setErr := connection.SetSessionConfigOption(ctx, &acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
				ConfigId: acp.SessionConfigId(modelOptionID), SessionId: created.SessionId, Value: acp.SessionConfigValueId(catalog.Models[index].ID),
			}})
			if setErr != nil {
				continue
			}
			catalog.Models[index].Reasoning = catalogFromConfig(response.ConfigOptions, nil).Reasoning
		}
	}
	catalog.SlashCommands = collector.commands()
	return catalog, nil
}

type catalogCollector struct {
	mu            sync.Mutex
	slashCommands []host.BackendSlashCommand
}

func (c *catalogCollector) SessionUpdate(_ context.Context, notification *acp.SessionNotification) error {
	if notification == nil || notification.Update.AvailableCommandsUpdate == nil {
		return nil
	}
	commands := make([]host.BackendSlashCommand, 0, len(notification.Update.AvailableCommandsUpdate.AvailableCommands))
	for _, command := range notification.Update.AvailableCommandsUpdate.AvailableCommands {
		inputHint := ""
		if command.Input != nil && command.Input.Unstructured != nil {
			inputHint = command.Input.Unstructured.Hint
		}
		commands = append(commands, host.BackendSlashCommand{Name: command.Name, Description: command.Description, InputHint: inputHint})
	}
	c.mu.Lock()
	c.slashCommands = commands
	c.mu.Unlock()
	return nil
}

func (c *catalogCollector) commands() []host.BackendSlashCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]host.BackendSlashCommand(nil), c.slashCommands...)
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
	s.startupMu.Lock()
	backendID := s.backendID
	if backendID == "" {
		s.startup = append(s.startup, *notification)
		s.startupMu.Unlock()
		return nil
	}
	s.startupMu.Unlock()
	if string(notification.SessionId) != "" && string(notification.SessionId) != backendID {
		s.observeForkUpdate(string(notification.SessionId), notification.Update)
		return nil
	}
	s.emitUpdate(notification.Update)
	return nil
}

func (s *managedSession) setBackendID(ctx context.Context, backendID string) {
	s.startupMu.Lock()
	s.backendID = backendID
	updates := append([]acp.SessionNotification(nil), s.startup...)
	s.startup = nil
	s.startupMu.Unlock()
	for index := range updates {
		_ = s.SessionUpdate(ctx, &updates[index])
	}
}

func (s *managedSession) observe(_ context.Context, direction client.Direction, raw json.RawMessage) error {
	if direction != client.DirectionInbound {
		return nil
	}
	var message struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &message); err != nil {
		return err
	}
	if len(message.Result) > 0 {
		var result map[string]any
		if json.Unmarshal(message.Result, &result) == nil && len(mapValue(result, "thread")) > 0 {
			s.emitEvent(host.Event{Type: host.EventTraceUpdated, Data: map[string]any{"session_history": result}})
		}
	}
	if message.Method != acp.MethodSessionUpdate {
		return nil
	}
	var params map[string]any
	if err := json.Unmarshal(message.Params, &params); err != nil {
		return err
	}
	update := mapValue(params, "update")
	discriminator := stringValue(update, "sessionUpdate")
	if discriminator == "done" && s.adapter.config.CompleteOnDone {
		s.scheduleDoneCompletion()
		return nil
	}
	if event, ok := legacySessionUpdateEvent(params, update, discriminator); ok {
		s.emitEvent(event)
		return nil
	}
	if knownSessionUpdate(discriminator) {
		return nil
	}
	s.emitEvent(host.Event{Type: host.EventTraceUpdated, Data: map[string]any{"provider_method": message.Method, "params": params}})
	return nil
}

func legacySessionUpdateEvent(params, update map[string]any, discriminator string) (host.Event, bool) {
	backendID := stringValue(params, "sessionId")
	switch discriminator {
	case "plan_update":
		message := stringAtValue(update, "plan", "content")
		if message == "" {
			message = FirstNonEmpty(stringValue(update, "content"), stringValue(update, "message"))
		}
		return host.Event{Type: host.EventPlanUpdate, Message: message, BackendSessionID: backendID, Data: params}, true
	case "plan_removed":
		return host.Event{Type: host.EventPlanUpdate, Message: "Plan removed.", BackendSessionID: backendID, Data: params}, true
	case "error":
		return host.Event{Type: host.EventTurnFailed, Message: stringValue(update, "message"), BackendSessionID: backendID, Data: params}, true
	default:
		return host.Event{}, false
	}
}

func (s *managedSession) scheduleDoneCompletion() {
	turnID := s.currentTurnID()
	if turnID == "" {
		return
	}
	go func() {
		if grace := s.adapter.config.DoneCompletionGrace; grace > 0 {
			time.Sleep(grace)
		}
		s.interactionMu.Lock()
		pending := len(s.interactions) > 0
		s.interactionMu.Unlock()
		s.toolMu.Lock()
		tools := s.toolActive
		s.toolMu.Unlock()
		if pending || tools {
			return
		}
		cancel := s.takeTurnIf(turnID)
		if cancel == nil {
			return
		}
		cancel()
		s.promptMu.Lock()
		finished := s.currentTurnID() == ""
		s.promptMu.Unlock()
		if !finished {
			return
		}
		s.emitEvent(host.Event{Type: host.EventTurnComplete, BackendTurnID: turnID, Data: map[string]any{"stop_reason": "done"}})
	}()
}

func knownSessionUpdate(value string) bool {
	switch value {
	case "user_message_chunk", "agent_message_chunk", "agent_thought_chunk", "tool_call", "tool_call_update", "plan", "available_commands_update", "current_mode_update", "config_option_update", "session_info_update", "usage_update":
		return true
	default:
		return false
	}
}

func (s *managedSession) RequestPermission(ctx context.Context, request *acp.RequestPermissionRequest) (*acp.RequestPermissionResponse, error) {
	if request == nil {
		return nil, errors.New("acpx: nil permission request")
	}
	if string(request.SessionId) != "" && string(request.SessionId) != s.backendID {
		return s.forkPermission(request), nil
	}
	id, response := s.awaitInteraction(client.RequestID(ctx), host.InteractionRequest{
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

func (s *managedSession) CreateElicitation(ctx context.Context, request *acp.CreateElicitationRequest) (*acp.CreateElicitationResponse, error) {
	if request == nil {
		return nil, errors.New("acpx: nil elicitation request")
	}
	kind, title, message := elicitationDetails(request)
	id, response := s.awaitInteraction(client.RequestID(ctx), host.InteractionRequest{
		Kind:    kind,
		Title:   title,
		Message: message,
		Fields:  elicitationFields(request),
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

func (s *managedSession) ElicitationComplete(_ context.Context, notification *acp.CompleteElicitationNotification) error {
	if notification != nil {
		s.emitEvent(host.Event{Type: host.EventTraceUpdated, Data: map[string]any{"elicitation_complete": valueMap(*notification)}})
	}
	return nil
}

func (s *managedSession) observeForkUpdate(sessionID string, update acp.SessionUpdate) {
	if update.ToolCall == nil {
		return
	}
	data := valueMap(*update.ToolCall)
	toolName := strings.TrimSpace(FirstNonEmpty(
		stringValue(mapValue(mapValue(data, "_meta"), "claudeCode"), "toolName"),
		stringValue(data, "toolName"),
		update.ToolCall.Title,
	))
	s.forkMu.Lock()
	if policy := s.forks[sessionID]; policy != nil {
		policy.tools[string(update.ToolCall.ToolCallId)] = toolName
	}
	s.forkMu.Unlock()
}

func (s *managedSession) forkPermission(request *acp.RequestPermissionRequest) *acp.RequestPermissionResponse {
	s.forkMu.Lock()
	policy := s.forks[string(request.SessionId)]
	toolName := ""
	if policy != nil {
		toolName = policy.tools[string(request.ToolCall.ToolCallId)]
	}
	allowed := false
	for _, prefix := range policyPrefixes(policy) {
		if strings.HasPrefix(toolName, prefix) {
			allowed = true
			break
		}
	}
	s.forkMu.Unlock()
	if !allowed {
		return &acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}
	}
	optionID := choosePermissionOption(host.InteractionResponse{Action: "approve"}, request.Options)
	if optionID == "" {
		return &acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}
	}
	return &acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(optionID)}}}
}

func policyPrefixes(policy *forkInteractionPolicy) []string {
	if policy == nil {
		return nil
	}
	return policy.allowedToolPrefixes
}

func (s *managedSession) ExtensionRequest(ctx context.Context, method string, params json.RawMessage) (any, error) {
	var payload any
	if err := json.Unmarshal(params, &payload); err != nil {
		return nil, err
	}
	data := map[string]any{"method": method, "params": payload}
	kind := host.InteractionForm
	lower := strings.ToLower(method)
	switch {
	case strings.Contains(lower, "permission") || strings.Contains(lower, "approval"):
		kind = host.InteractionPermission
	case strings.Contains(lower, "plan"):
		kind = host.InteractionPlan
	case strings.Contains(lower, "question") || strings.Contains(lower, "ask"):
		kind = host.InteractionChoice
	}
	id, response := s.awaitInteraction(client.RequestID(ctx), host.InteractionRequest{Kind: kind, Title: method, Data: data})
	defer s.removeInteraction(id)
	select {
	case answer := <-response:
		if answer.Values != nil {
			if result, ok := answer.Values["_result"]; ok && len(answer.Values) == 1 {
				return result, nil
			}
			return answer.Values, nil
		}
		return map[string]any{
			"action":   answer.Action,
			"optionId": answer.OptionID,
			"message":  answer.Message,
		}, nil
	case <-s.done:
		return map[string]any{"action": "cancel"}, nil
	}
}

func (s *managedSession) ExtensionNotification(_ context.Context, method string, params json.RawMessage) error {
	var payload any
	if err := json.Unmarshal(params, &payload); err != nil {
		return err
	}
	s.emitEvent(host.Event{Type: host.EventTraceUpdated, Data: map[string]any{"extension_method": method, "params": payload}})
	return nil
}

func (s *managedSession) awaitInteraction(preferredID string, request host.InteractionRequest) (string, <-chan host.InteractionResponse) {
	s.interactionMu.Lock()
	id := strings.TrimSpace(preferredID)
	if id == "" || s.interactions[id] != nil {
		s.interactionID++
		id = fmt.Sprintf("interaction-%d", s.interactionID)
	}
	request.ID = id
	response := make(chan host.InteractionResponse, 1)
	s.interactions[id] = &pendingInteraction{request: request, response: response}
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
	responses := make([]*pendingInteraction, 0, len(s.interactions))
	for id, pending := range s.interactions {
		delete(s.interactions, id)
		responses = append(responses, pending)
	}
	s.interactionMu.Unlock()
	for _, pending := range responses {
		select {
		case pending.response <- host.InteractionResponse{RequestID: pending.request.ID, Action: "cancel"}:
		default:
		}
	}
}

func (s *managedSession) stop() {
	s.doneOnce.Do(func() {
		close(s.done)
		s.cancelTurn()
		s.cancelInteractions()
	})
}

func (s *managedSession) emitEvent(event host.Event) {
	if s.emit == nil {
		return
	}
	event.Backend = s.adapter.Backend()
	if event.BackendTurnID == "" {
		event.BackendTurnID = s.currentTurnID()
	}
	if event.BackendSessionID == "" {
		event.BackendSessionID = s.backendID
	}
	if event.BackendThreadID == "" {
		event.BackendThreadID = s.backendID
	}
	s.markReplay(&event)
	s.emit(event)
}

func (s *managedSession) beginReplay() {
	s.replayMu.Lock()
	s.replaying = true
	s.replayFirst = true
	s.replayMu.Unlock()
}

func (s *managedSession) endReplay() {
	s.replayMu.Lock()
	s.replaying = false
	s.replayMu.Unlock()
}

func (s *managedSession) markReplay(event *host.Event) {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	if !s.replaying {
		return
	}
	if event.Local == nil {
		event.Local = map[string]any{}
	}
	event.Local["acpx.replay"] = true
	if s.replayFirst {
		event.Local["acpx.replay_start"] = true
		s.replayFirst = false
	}
}

func (s *managedSession) setTurn(turnID string, cancel context.CancelFunc) {
	s.turnMu.Lock()
	s.turnID = turnID
	s.turnCancel = cancel
	s.turnMu.Unlock()
}

func (s *managedSession) finishTurn(turnID string) bool {
	s.turnMu.Lock()
	if s.turnID != turnID {
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

func (s *managedSession) cancelTurn() {
	s.turnMu.Lock()
	cancel := s.turnCancel
	s.turnMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *managedSession) takeTurn() (string, context.CancelFunc) {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	turnID := s.turnID
	cancel := s.turnCancel
	s.turnID = ""
	s.turnCancel = nil
	return turnID, cancel
}

func (s *managedSession) takeTurnIf(turnID string) context.CancelFunc {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if s.turnID != turnID {
		return nil
	}
	cancel := s.turnCancel
	s.turnID = ""
	s.turnCancel = nil
	return cancel
}

func (s *managedSession) currentTurnID() string {
	s.turnMu.RLock()
	defer s.turnMu.RUnlock()
	return s.turnID
}

func (s *managedSession) emitUpdate(update acp.SessionUpdate) {
	switch {
	case update.UserMessageChunk != nil:
		message := contentText(update.UserMessageChunk.Content)
		if message != "" {
			data := valueMap(*update.UserMessageChunk)
			data["delta"] = message
			s.emitEvent(host.Event{Type: host.EventMessage, Role: "user", Message: message, Data: data})
		}
	case update.AgentMessageChunk != nil:
		message := contentText(update.AgentMessageChunk.Content)
		if message != "" {
			data := valueMap(*update.AgentMessageChunk)
			data["delta"] = message
			s.emitEvent(host.Event{Type: host.EventMessage, Role: "assistant", Message: message, Data: data})
		}
	case update.AgentThoughtChunk != nil:
		message := contentText(update.AgentThoughtChunk.Content)
		if message != "" {
			s.emitEvent(host.Event{Type: host.EventThinking, Message: message, Data: map[string]any{"delta": message}})
		}
	case update.ToolCall != nil:
		data := valueMap(*update.ToolCall)
		display := ToolDisplayFromACP(data, string(update.ToolCall.Status))
		s.rememberTool(string(update.ToolCall.ToolCallId), display)
		s.toolMu.Lock()
		s.toolActive = true
		s.toolMu.Unlock()
		s.emitEvent(host.Event{Type: host.EventToolStarted, ToolDisplay: display, Data: normalizedToolData(data, update.ToolCall.Content)})
	case update.ToolCallUpdate != nil:
		status := acp.ToolCallStatus("")
		if update.ToolCallUpdate.Status != nil {
			status = *update.ToolCallUpdate.Status
		}
		data := valueMap(*update.ToolCallUpdate)
		display := s.mergeTool(string(update.ToolCallUpdate.ToolCallId), ToolDisplayFromACP(data, string(status)))
		s.emitEvent(host.Event{Type: host.EventToolOutput, Message: toolContentText(update.ToolCallUpdate.Content, update.ToolCallUpdate.RawOutput), ToolDisplay: display, Data: normalizedToolData(data, update.ToolCallUpdate.Content)})
		if display != nil && display.Status == "completed" && (display.Kind == "edit" || display.Kind == "delete" || display.Kind == "move") && display.Target != "" {
			s.emitEvent(host.Event{Type: host.EventFileChanged, Data: map[string]any{"path": display.Target, "tool_call_id": display.ID}})
		}
	case update.Plan != nil:
		data := valueMap(*update.Plan)
		data["todos"] = normalizedPlanEntries(update.Plan.Entries)
		s.emitEvent(host.Event{Type: host.EventTodoUpdate, Data: data})
	case update.AvailableCommandsUpdate != nil:
		commands := make([]host.BackendSlashCommand, 0, len(update.AvailableCommandsUpdate.AvailableCommands))
		for _, command := range update.AvailableCommandsUpdate.AvailableCommands {
			inputHint := ""
			if command.Input != nil && command.Input.Unstructured != nil {
				inputHint = command.Input.Unstructured.Hint
			}
			commands = append(commands, host.BackendSlashCommand{Name: command.Name, Description: command.Description, InputHint: inputHint})
		}
		s.emitEvent(host.Event{Type: host.EventAvailableCommands, Data: map[string]any{"available_commands": commands}})
	case update.ConfigOptionUpdate != nil:
		s.emitConfig(update.ConfigOptionUpdate.ConfigOptions, nil)
	case update.CurrentModeUpdate != nil:
		s.emitCurrentMode(update.CurrentModeUpdate.CurrentModeId)
	case update.UsageUpdate != nil:
		s.emitEvent(host.Event{Type: host.EventTraceUpdated, Data: valueMap(*update.UsageUpdate)})
	}
}

func (s *managedSession) rememberTool(id string, display *host.ToolDisplay) {
	if id == "" || display == nil {
		return
	}
	s.toolMu.Lock()
	if s.tools == nil {
		s.tools = map[string]toolState{}
	}
	s.tools[id] = toolState{kind: display.Kind, target: display.Target}
	s.toolMu.Unlock()
}

func (s *managedSession) mergeTool(id string, display *host.ToolDisplay) *host.ToolDisplay {
	if display == nil {
		display = &host.ToolDisplay{ID: id}
	}
	s.toolMu.Lock()
	previous := s.tools[id]
	if display.Kind == "" {
		display.Kind = previous.kind
	}
	if display.Target == "" {
		display.Target = previous.target
	}
	if display.Title == "" {
		display.Title = DefaultToolTitle(display.Kind, display.Command)
	}
	if display.Status == "completed" || display.Status == "failed" {
		delete(s.tools, id)
	} else {
		s.tools[id] = toolState{kind: display.Kind, target: display.Target}
	}
	s.toolMu.Unlock()
	return display
}

func (s *managedSession) clearTools() {
	s.toolMu.Lock()
	s.tools = map[string]toolState{}
	s.toolActive = false
	s.toolMu.Unlock()
}

func normalizedPlanEntries(entries []acp.PlanEntry) []map[string]string {
	result := make([]map[string]string, 0, len(entries))
	for _, entry := range entries {
		content := strings.TrimSpace(entry.Content)
		status := strings.TrimSpace(string(entry.Status))
		if content == "" || (status != "pending" && status != "in_progress" && status != "completed") {
			continue
		}
		result = append(result, map[string]string{"content": content, "status": status})
	}
	return result
}

func normalizedToolData(data map[string]any, content []acp.ToolCallContent) map[string]any {
	if data == nil {
		data = map[string]any{}
	}
	images := toolContentImages(content)
	if len(images) > 0 {
		data["images"] = images
	}
	return data
}

func toolContentImages(content []acp.ToolCallContent) []map[string]any {
	var images []map[string]any
	for _, item := range content {
		if item.Content == nil || item.Content.Content.Image == nil {
			continue
		}
		image := item.Content.Content.Image
		if image.Data == "" {
			continue
		}
		mimeType := image.MimeType
		if mimeType == "" {
			mimeType = "image/png"
		}
		images = append(images, map[string]any{"mime_type": mimeType, "data_base64": image.Data})
	}
	return images
}

func toolContentText(content []acp.ToolCallContent, rawOutput any) string {
	var parts []string
	for _, item := range content {
		if item.Content != nil {
			if text := contentText(item.Content.Content); text != "" && text != "[image]" {
				parts = append(parts, text)
			}
		}
		if item.Diff != nil && item.Diff.Path != "" {
			parts = append(parts, item.Diff.Path)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n")
	}
	switch value := rawOutput.(type) {
	case string:
		return value
	case nil:
		return ""
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return ""
		}
		return string(raw)
	}
}

func (s *managedSession) emitConfig(options []acp.SessionConfigOption, modes *acp.SessionModeState) {
	s.updateControls(options, modes)
	catalog := catalogFromConfig(options, modes)
	data := map[string]any{"catalog": catalog, "config_options": valueMap(options)}
	model, reasoning, configMode := currentSelections(options)
	if model != "" {
		data["current_model"] = model
	}
	if reasoning != "" {
		data["current_reasoning"] = reasoning
	}
	if configMode != "" {
		data["current_mode"] = configMode
	}
	if modes != nil {
		data["current_mode"] = string(modes.CurrentModeId)
	}
	s.emitEvent(host.Event{Type: host.EventConfigCatalog, Data: data})
	if len(catalog.Models) > 0 {
		s.emitEvent(host.Event{Type: host.EventModels, Data: map[string]any{"models": catalog.Models, "current_model": model}})
	}
	if len(catalog.PermissionModes) > 0 {
		current := configMode
		if modes != nil {
			current = string(modes.CurrentModeId)
		}
		s.emitEvent(host.Event{Type: host.EventPermissionModes, Data: map[string]any{"permission_modes": catalog.PermissionModes, "current_mode": current}})
	}
	if len(catalog.Reasoning) > 0 {
		s.emitEvent(host.Event{Type: host.EventReasoningLevels, Data: map[string]any{"reasoning": catalog.Reasoning, "current_reasoning": reasoning}})
	}
}

func (s *managedSession) applySelections(ctx context.Context, model, reasoning, mode string) error {
	selections := []struct {
		kind  string
		value string
		keys  []string
	}{
		{kind: "model", value: strings.TrimSpace(model), keys: []string{"model"}},
		{kind: "reasoning", value: strings.TrimSpace(reasoning), keys: []string{"thought_level", "reasoning", "reasoning_effort", "effort"}},
	}
	var joined error
	for _, selection := range selections {
		if selection.value == "" || selection.value == s.selected(selection.kind) {
			continue
		}
		optionID := findOption(s.configOptions(), selection.keys...)
		if optionID == "" {
			s.setSelected(selection.kind, selection.value)
			continue
		}
		response, err := s.conn.SetSessionConfigOption(ctx, &acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
			ConfigId:  acp.SessionConfigId(optionID),
			SessionId: acp.SessionId(s.backendID),
			Value:     acp.SessionConfigValueId(selection.value),
		}})
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("acpx: set %s: %w", selection.kind, err))
			continue
		}
		s.setSelected(selection.kind, selection.value)
		s.emitAppliedSelection(selection.kind, selection.value, response.ConfigOptions)
	}
	mode = strings.TrimSpace(mode)
	if mode == "" || mode == s.selected("mode") {
		return s.configurationResult(joined)
	}
	modes := s.modeState()
	if (modes != nil && len(modes.AvailableModes) > 0) || containsString(s.adapter.config.SessionModeValues, mode) {
		if _, err := s.conn.SetSessionMode(ctx, &acp.SetSessionModeRequest{SessionId: acp.SessionId(s.backendID), ModeId: acp.SessionModeId(mode)}); err != nil {
			return s.configurationResult(errors.Join(joined, fmt.Errorf("acpx: set mode: %w", err)))
		}
		s.setSelected("mode", mode)
		s.emitCurrentMode(acp.SessionModeId(mode))
		return s.configurationResult(joined)
	}
	optionID := findOption(s.configOptions(), "mode", "permission_mode")
	if optionID == "" {
		s.setSelected("mode", mode)
		return s.configurationResult(joined)
	}
	response, err := s.conn.SetSessionConfigOption(ctx, &acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
		ConfigId: acp.SessionConfigId(optionID), SessionId: acp.SessionId(s.backendID), Value: acp.SessionConfigValueId(mode),
	}})
	if err != nil {
		return s.configurationResult(errors.Join(joined, fmt.Errorf("acpx: set mode: %w", err)))
	}
	s.setSelected("mode", mode)
	s.emitAppliedSelection("mode", mode, response.ConfigOptions)
	return s.configurationResult(joined)
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if strings.TrimSpace(candidate) == value {
			return true
		}
	}
	return false
}

func (s *managedSession) configurationResult(err error) error {
	if err == nil || !s.adapter.config.BestEffortConfiguration {
		return err
	}
	s.emitEvent(host.Event{Type: host.EventTraceUpdated, Message: err.Error(), Data: map[string]any{"configuration_error": err.Error()}})
	return nil
}

func (s *managedSession) emitAppliedSelection(kind, value string, options []acp.SessionConfigOption) {
	s.emitConfig(options, nil)
	catalog := catalogFromConfig(options, nil)
	model, reasoning, mode := currentSelections(options)
	switch kind {
	case "model":
		if model == "" {
			s.emitEvent(host.Event{Type: host.EventModels, Data: map[string]any{"current_model": value}})
		}
		if len(catalog.Reasoning) == 0 {
			s.emitEvent(host.Event{Type: host.EventReasoningLevels, Data: map[string]any{"reasoning": []host.BackendReasoning{}, "current_reasoning": ""}})
		}
	case "reasoning":
		if reasoning == "" {
			s.emitEvent(host.Event{Type: host.EventReasoningLevels, Data: map[string]any{"current_reasoning": value}})
		}
	case "mode":
		if mode == "" {
			s.emitEvent(host.Event{Type: host.EventPermissionModes, Data: map[string]any{"current_mode": value}})
		}
	}
}

func (s *managedSession) initializeSelections(options []acp.SessionConfigOption, modes *acp.SessionModeState) {
	model, reasoning, mode := currentSelections(options)
	if modes != nil {
		mode = string(modes.CurrentModeId)
	}
	s.configMu.Lock()
	s.model = model
	s.reasoning = reasoning
	s.mode = mode
	s.configMu.Unlock()
}

func (s *managedSession) selected(kind string) string {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	switch kind {
	case "model":
		return s.model
	case "reasoning":
		return s.reasoning
	case "mode":
		return s.mode
	default:
		return ""
	}
}

func (s *managedSession) setSelected(kind, value string) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	switch kind {
	case "model":
		s.model = value
	case "reasoning":
		s.reasoning = value
	case "mode":
		s.mode = value
	}
}

func (s *managedSession) updateControls(options []acp.SessionConfigOption, modes *acp.SessionModeState) {
	s.configMu.Lock()
	if options != nil {
		s.options = append([]acp.SessionConfigOption(nil), options...)
	}
	if modes != nil {
		cloned := *modes
		cloned.AvailableModes = append([]acp.SessionMode(nil), modes.AvailableModes...)
		s.modes = &cloned
	}
	s.configMu.Unlock()
}

func (s *managedSession) configOptions() []acp.SessionConfigOption {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return append([]acp.SessionConfigOption(nil), s.options...)
}

func (s *managedSession) modeState() *acp.SessionModeState {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	if s.modes == nil {
		return nil
	}
	cloned := *s.modes
	cloned.AvailableModes = append([]acp.SessionMode(nil), s.modes.AvailableModes...)
	return &cloned
}

func (s *managedSession) emitCurrentMode(mode acp.SessionModeId) {
	s.configMu.Lock()
	if s.modes != nil {
		s.modes.CurrentModeId = mode
	}
	s.configMu.Unlock()
	s.emitEvent(host.Event{Type: host.EventConfigCatalog, Data: map[string]any{"current_mode": string(mode)}})
	s.emitEvent(host.Event{Type: host.EventPermissionModes, Data: map[string]any{"current_mode": string(mode)}})
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

func elicitationFields(request *acp.CreateElicitationRequest) []host.InteractionField {
	if request == nil || request.Form == nil {
		return nil
	}
	schema := request.Form.RequestedSchema
	required := make(map[string]bool, len(schema.Required))
	order := make([]string, 0, len(schema.Properties))
	seen := map[string]bool{}
	for _, name := range schema.Required {
		if _, ok := schema.Properties[name]; ok && !seen[name] {
			required[name] = true
			seen[name] = true
			order = append(order, name)
		}
	}
	optional := make([]string, 0, len(schema.Properties)-len(order))
	for name := range schema.Properties {
		if !seen[name] {
			optional = append(optional, name)
		}
	}
	sort.Strings(optional)
	order = append(order, optional...)
	fields := make([]host.InteractionField, 0, len(order))
	for _, name := range order {
		if strings.HasSuffix(name, "__note") {
			continue
		}
		property, _ := schema.Properties[name].(map[string]any)
		label := strings.TrimSpace(stringValue(property, "title"))
		if label == "" {
			label = name
		}
		options := elicitationOptions(property)
		fieldType := strings.ToLower(strings.TrimSpace(stringValue(property, "type")))
		_, hasNote := schema.Properties[name+"__note"]
		fields = append(fields, host.InteractionField{
			ID:            name,
			Label:         label,
			Description:   strings.TrimSpace(stringValue(property, "description")),
			Required:      required[name],
			Options:       options,
			AllowFreeText: hasNote || len(options) == 0 && (fieldType == "" || fieldType == "string"),
		})
	}
	return fields
}

func elicitationOptions(property map[string]any) []host.InteractionOption {
	var options []host.InteractionOption
	for _, raw := range anyValues(property["oneOf"]) {
		item, _ := raw.(map[string]any)
		id := strings.TrimSpace(FirstNonEmpty(stringValue(item, "const"), stringValue(item, "value"), stringValue(item, "id")))
		if id == "" {
			continue
		}
		label := strings.TrimSpace(FirstNonEmpty(stringValue(item, "title"), stringValue(item, "label"), id))
		options = append(options, host.InteractionOption{ID: id, Label: label, Description: strings.TrimSpace(stringValue(item, "description"))})
	}
	if len(options) > 0 {
		return options
	}
	for _, raw := range anyValues(property["enum"]) {
		id, _ := raw.(string)
		id = strings.TrimSpace(id)
		if id != "" {
			options = append(options, host.InteractionOption{ID: id, Label: id})
		}
	}
	return options
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

func currentSelections(options []acp.SessionConfigOption) (string, string, string) {
	var model, reasoning, mode string
	for _, option := range options {
		if option.Select == nil {
			continue
		}
		id := string(option.Select.Id)
		category := ""
		if option.Select.Category != nil {
			category = string(*option.Select.Category)
		}
		current := string(option.Select.CurrentValue)
		switch {
		case category == "model" || id == "model":
			model = current
		case category == "mode" || id == "mode" || id == "permission_mode":
			mode = current
		case category == "thought_level" || id == "reasoning" || id == "reasoning_effort" || id == "effort":
			reasoning = current
		}
	}
	return model, reasoning, mode
}

func catalogFromConfig(options []acp.SessionConfigOption, modes *acp.SessionModeState) host.BackendCatalog {
	catalog := host.BackendCatalog{}
	modeIDs := map[string]bool{}
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
				if !modeIDs[item.ID] {
					catalog.PermissionModes = append(catalog.PermissionModes, host.BackendPermissionMode{ID: item.ID, Label: item.Label})
					modeIDs[item.ID] = true
				}
			case category == "thought_level" || id == "reasoning" || id == "reasoning_effort" || id == "effort":
				catalog.Reasoning = append(catalog.Reasoning, host.BackendReasoning{ID: item.ID, Label: item.Label})
			}
		}
	}
	if modes != nil {
		for _, mode := range modes.AvailableModes {
			id := string(mode.Id)
			if !modeIDs[id] {
				catalog.PermissionModes = append(catalog.PermissionModes, host.BackendPermissionMode{ID: id, Label: mode.Name})
				modeIDs[id] = true
			}
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
