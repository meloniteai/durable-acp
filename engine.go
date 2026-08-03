// Package durableacp provides the batteries-included, embeddable entry point
// for durable ACP agent sessions.
package durableacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/meloniteai/durable-acp/adapters"
	"github.com/meloniteai/durable-acp/host"
	"github.com/meloniteai/durable-acp/journal"
	"github.com/meloniteai/durable-acp/runtime"
	"github.com/meloniteai/durable-acp/session"
	"github.com/meloniteai/durable-acp/worktree"
)

const configFileName = "config.json"

// Settings are persisted in config.json. Home ownership is intentionally not
// represented here: the embedding application supplies it to Open.
type Settings struct {
	Version      int              `json:"version"`
	Worktrees    WorktreeSettings `json:"worktrees"`
	CatalogCache bool             `json:"catalog_cache"`
}

// WorktreeSettings control managed Git isolation.
type WorktreeSettings struct {
	BranchPrefix string `json:"branch_prefix"`
}

// DefaultSettings are used when config.json does not yet exist.
func DefaultSettings() Settings {
	return Settings{
		Version:      1,
		Worktrees:    WorktreeSettings{BranchPrefix: "durable-acp"},
		CatalogCache: true,
	}
}

// Option customizes Engine startup. Only durable-acp settings, such as a
// branch prefix, are persisted; host-specific values remain in the host.
type Option func(*openOptions)

type openOptions struct {
	adapters         []host.Adapter
	listeners        []host.EventSink
	branchPrefix     string
	journal          JournalConfiguration
	runtime          runtime.Config
	hasRuntimeConfig bool
}

// JournalConfiguration selects the Engine journal without introducing a
// persistence-provider abstraction. Store remains owned by the caller;
// Directory stores are opened and closed by the Engine.
type JournalConfiguration struct {
	Store                 *journal.Store
	Directory             string
	Options               []journal.Option
	DisableRuntimeJournal bool
}

// WithAdapters adds adapters or replaces a built-in adapter with the same
// backend name. Explicit adapters always win.
func WithAdapters(adapters ...host.Adapter) Option {
	return func(options *openOptions) {
		options.adapters = append(options.adapters, adapters...)
	}
}

// WithEventSink subscribes to normalized runtime events.
func WithEventSink(listener host.EventSink) Option {
	return func(options *openOptions) {
		if listener != nil {
			options.listeners = append(options.listeners, listener)
		}
	}
}

// WithBranchPrefix overrides the persisted managed-branch prefix for this
// process. It is useful to hosts that isolate several engines under one home.
func WithBranchPrefix(prefix string) Option {
	return func(options *openOptions) {
		options.branchPrefix = strings.TrimSpace(prefix)
	}
}

// WithJournalStore uses a caller-owned journal store.
func WithJournalStore(store *journal.Store) Option {
	return func(options *openOptions) {
		options.journal.Store = store
	}
}

// WithJournalConfiguration uses a caller-owned store or an Engine-owned
// directory with journal options.
func WithJournalConfiguration(configuration JournalConfiguration) Option {
	return func(options *openOptions) {
		configuration.Options = append([]journal.Option(nil), configuration.Options...)
		options.journal = configuration
	}
}

// WithRuntimeConfiguration supplies provider-neutral runtime behavior. The
// Engine always supplies its selected journal and event publisher.
func WithRuntimeConfiguration(configuration runtime.Config) Option {
	return func(options *openOptions) {
		options.runtime = configuration
		options.hasRuntimeConfig = true
	}
}

// Engine owns one supplied home directory and all durable agent state below it.
type Engine struct {
	home         string
	settings     Settings
	journal      *journal.Store
	journalOwned bool
	runtime      *runtime.Runtime
	worktrees    *worktree.Manager

	mu         sync.Mutex
	listeners  map[uint64]host.EventSink
	nextSub    uint64
	closers    []io.Closer
	closeOnce  sync.Once
	closeErr   error
	manifestMu sync.Mutex
}

// WorkspaceMode selects whether Start creates a managed worktree or uses a
// worktree supplied by the embedding application.
type WorkspaceMode string

const (
	WorkspaceManaged  WorkspaceMode = "managed"
	WorkspaceExisting WorkspaceMode = "existing"
)

// StartRequest creates and starts a session in one operation.
type StartRequest struct {
	ID                     string
	Backend                host.Backend
	WorkspaceMode          WorkspaceMode
	Source                 string
	Worktree               string
	Prompt                 string
	Attachments            []host.Attachment
	Model                  string
	Reasoning              string
	PermissionMode         string
	ResumeBackendSessionID string
	Ext                    json.RawMessage
}

// Configuration is the effective provider-neutral session configuration.
type Configuration = runtime.Configuration

// Session is the durable Engine-level record for a runtime session.
type Session struct {
	ID             string              `json:"id"`
	Backend        host.Backend        `json:"backend"`
	WorkspaceMode  WorkspaceMode       `json:"workspace_mode"`
	Worktree       worktree.Session    `json:"worktree"`
	BackendSession host.BackendSession `json:"backend_session,omitzero"`
	Configuration  Configuration       `json:"configuration"`
	Ext            json.RawMessage     `json:"ext,omitempty"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
	ClosedAt       *time.Time          `json:"closed_at,omitempty"`
}

// LifecycleSnapshot is the lifecycle portion of an Engine snapshot.
type LifecycleSnapshot struct {
	Status    session.Status `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	ClosedAt  *time.Time     `json:"closed_at,omitempty"`
}

// WorkspaceSnapshot is the workspace portion of an Engine snapshot.
type WorkspaceSnapshot struct {
	Mode      WorkspaceMode    `json:"mode"`
	Workspace worktree.Session `json:"workspace"`
}

// BackendSnapshot contains stable provider references.
type BackendSnapshot struct {
	Backend host.Backend        `json:"backend"`
	Session host.BackendSession `json:"session,omitzero"`
}

// ActiveTurnSnapshot contains the current turn identity.
type ActiveTurnSnapshot struct {
	Active bool   `json:"active"`
	ID     string `json:"id,omitempty"`
}

// QueueSnapshot contains the current ordered turn queue.
type QueueSnapshot struct {
	Depth           int                  `json:"depth"`
	Entries         []runtime.QueueEntry `json:"entries,omitempty"`
	DispatchBlocked bool                 `json:"dispatch_blocked,omitempty"`
}

// Snapshot is the complete provider-neutral Engine view of one session.
type Snapshot struct {
	SessionID           string                   `json:"session_id"`
	Lifecycle           LifecycleSnapshot        `json:"lifecycle"`
	Workspace           WorkspaceSnapshot        `json:"workspace"`
	Backend             BackendSnapshot          `json:"backend"`
	ActiveTurn          ActiveTurnSnapshot       `json:"active_turn"`
	Queue               QueueSnapshot            `json:"queue"`
	PendingInteraction  *host.InteractionRequest `json:"pending_interaction,omitempty"`
	Configuration       Configuration            `json:"configuration"`
	LastJournalSequence uint64                   `json:"last_journal_sequence"`
	Ext                 json.RawMessage          `json:"ext,omitempty"`
}

// Open prepares the supplied absolute home directory and loads its settings.
// The caller owns home selection; Open never uses a home environment variable.
//
//nolint:govet // Scoped filesystem errors keep each preparation step local.
func Open(ctx context.Context, home string, options ...Option) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("durableacp: open: %w", err)
	}
	home = strings.TrimSpace(home)
	if home == "" {
		return nil, errors.New("durableacp: home directory is required")
	}
	if !filepath.IsAbs(home) {
		return nil, errors.New("durableacp: home directory must be absolute")
	}
	home = filepath.Clean(home)
	resolved := openOptions{}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	if resolved.journal.Store != nil && strings.TrimSpace(resolved.journal.Directory) != "" {
		return nil, errors.New("durableacp: journal store and directory are mutually exclusive")
	}
	if directory := strings.TrimSpace(resolved.journal.Directory); directory != "" && !filepath.IsAbs(directory) {
		return nil, errors.New("durableacp: journal directory must be absolute")
	}
	if err := secureDir(home); err != nil {
		return nil, fmt.Errorf("durableacp: prepare home: %w", err)
	}
	children := []string{"sessions", "logs/providers", "logs/sessions", "worktrees", "cache"}
	if resolved.journal.Store == nil && strings.TrimSpace(resolved.journal.Directory) == "" {
		children = append(children, "journals")
	}
	for _, child := range children {
		if err := secureDir(filepath.Join(home, child)); err != nil {
			return nil, fmt.Errorf("durableacp: prepare %s: %w", child, err)
		}
	}
	settings, err := loadSettings(home)
	if err != nil {
		return nil, err
	}
	if resolved.branchPrefix != "" {
		settings.Worktrees.BranchPrefix = resolved.branchPrefix
	}
	if err := normalizeSettings(&settings); err != nil {
		return nil, err
	}
	if err := saveSettings(home, settings); err != nil {
		return nil, err
	}

	store := resolved.journal.Store
	journalOwned := false
	if store == nil {
		directory := strings.TrimSpace(resolved.journal.Directory)
		if directory == "" {
			directory = filepath.Join(home, "journals")
		}
		store, err = journal.NewStore(filepath.Clean(directory), resolved.journal.Options...)
		if err != nil {
			return nil, err
		}
		journalOwned = true
	}
	manager, err := worktree.NewManager(worktree.Config{
		Root:         filepath.Join(home, "worktrees"),
		BranchPrefix: settings.Worktrees.BranchPrefix,
	})
	if err != nil {
		if journalOwned {
			_ = store.Close()
		}
		return nil, err
	}
	providerLogs, closers, err := openProviderLogs(home)
	if err != nil {
		if journalOwned {
			_ = store.Close()
		}
		return nil, err
	}
	engine := &Engine{
		home:         home,
		settings:     settings,
		journal:      store,
		journalOwned: journalOwned,
		worktrees:    manager,
		listeners:    map[uint64]host.EventSink{},
		closers:      closers,
	}
	for _, listener := range resolved.listeners {
		engine.Subscribe(listener)
	}
	configuredAdapters := append(adapters.Default(adapters.WithStderrFor(func(backend host.Backend) io.Writer {
		return providerLogs[backend]
	})), resolved.adapters...)
	runtimeConfig := resolved.runtime
	if !resolved.hasRuntimeConfig {
		runtimeConfig = runtime.Config{}
	}
	if resolved.journal.DisableRuntimeJournal {
		runtimeConfig.Journal = nil
	} else {
		runtimeConfig.Journal = store
	}
	runtimeConfig.EventSink = engine.publish
	hostTurnSubmitted := runtimeConfig.TurnSubmitted
	runtimeConfig.TurnSubmitted = func(submission runtime.TurnSubmission) {
		engine.recordTurnSubmission(submission)
		if hostTurnSubmitted != nil {
			hostTurnSubmitted(submission)
		}
	}
	if strings.TrimSpace(runtimeConfig.CatalogCacheDir) == "" {
		runtimeConfig.CatalogCacheDir = catalogDir(home, settings)
	}
	engine.runtime = runtime.New(runtimeConfig, mergeAdapters(configuredAdapters)...)
	return engine, nil
}

// Home returns the exact home directory supplied to Open.
func (e *Engine) Home() string {
	if e == nil {
		return ""
	}
	return e.home
}

// Settings returns the active, normalized settings.
func (e *Engine) Settings() Settings {
	if e == nil {
		return Settings{}
	}
	return e.settings
}

// Runtime exposes the provider-neutral lower-level runtime for advanced host
// integrations. Normal hosts should prefer Engine lifecycle methods.
func (e *Engine) Runtime() *runtime.Runtime {
	if e == nil {
		return nil
	}
	return e.runtime
}

// Subscribe receives normalized events until its returned unsubscribe function
// is called.
func (e *Engine) Subscribe(listener host.EventSink) func() {
	if e == nil || listener == nil {
		return func() {}
	}
	e.mu.Lock()
	e.nextSub++
	id := e.nextSub
	e.listeners[id] = listener
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		delete(e.listeners, id)
		e.mu.Unlock()
	}
}

// Start creates a durable session and starts its provider adapter.
func (e *Engine) Start(ctx context.Context, request StartRequest) (Session, error) {
	if e == nil || e.runtime == nil {
		return Session{}, errors.New("durableacp: engine is not open")
	}
	if strings.TrimSpace(string(request.Backend)) == "" {
		return Session{}, errors.New("durableacp: backend is required")
	}
	id := strings.TrimSpace(request.ID)
	if id == "" {
		var err error
		id, err = sessionID()
		if err != nil {
			return Session{}, err
		}
	}
	if !validSessionID(id) {
		return Session{}, fmt.Errorf("durableacp: invalid session ID %q", id)
	}
	if _, err := os.Stat(e.sessionPath(id)); err == nil {
		if strings.TrimSpace(request.ResumeBackendSessionID) == "" {
			return Session{}, fmt.Errorf("durableacp: session %q already exists", id)
		}
		return e.resumeStart(ctx, request)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Session{}, fmt.Errorf("durableacp: inspect session manifest: %w", err)
	}
	mode := request.WorkspaceMode
	if mode == "" {
		mode = WorkspaceManaged
	}
	now := time.Now().UTC()
	entry := Session{
		ID: id, Backend: request.Backend, WorkspaceMode: mode,
		Configuration: configurationFromStart(request), Ext: cloneRawMessage(request.Ext),
		CreatedAt: now, UpdatedAt: now,
	}
	switch mode {
	case WorkspaceManaged:
		managed, err := e.worktrees.Create(ctx, worktree.CreateRequest{ID: id, Source: request.Source})
		if err != nil {
			return Session{}, err
		}
		entry.Worktree = managed
	case WorkspaceExisting:
		inspected, err := e.inspectExistingWorkspace(ctx, id, request.Worktree)
		if err != nil {
			return Session{}, err
		}
		entry.Worktree = inspected
	default:
		return Session{}, fmt.Errorf("durableacp: unsupported workspace mode %q", mode)
	}
	if _, err := e.runtime.Create(ctx, runtime.CreateRequest{ID: id, Backend: request.Backend, Worktree: entry.Worktree.Path}); err != nil {
		e.rollbackStart(ctx, entry)
		return Session{}, err
	}
	if err := e.saveSession(&entry); err != nil {
		e.rollbackStart(ctx, entry)
		return Session{}, err
	}
	backendSession, err := e.runtime.Start(ctx, host.StartSessionRequest{
		SessionID:              id,
		Prompt:                 request.Prompt,
		Attachments:            request.Attachments,
		Model:                  request.Model,
		Reasoning:              request.Reasoning,
		PermissionMode:         request.PermissionMode,
		ResumeBackendSessionID: request.ResumeBackendSessionID,
		Ext:                    cloneRawMessage(request.Ext),
	})
	if err != nil {
		e.rollbackStart(ctx, entry)
		return Session{}, err
	}
	entry.BackendSession = backendSession
	if runtimeState, stateErr := e.runtime.State(id); stateErr == nil {
		entry.Configuration = runtimeState.Configuration
	}
	if err := e.saveSession(&entry); err != nil {
		return Session{}, err
	}
	return entry, nil
}

// Resume restores a persisted session and starts its provider with the last
// durable backend session ID. It performs no implicit worktree creation.
func (e *Engine) Resume(ctx context.Context, id string) (Session, error) {
	return e.resumeStart(ctx, StartRequest{ID: id})
}

func (e *Engine) resumeStart(ctx context.Context, request StartRequest) (Session, error) {
	entry, err := e.loadSession(request.ID)
	if err != nil {
		return Session{}, err
	}
	if entry.ClosedAt != nil {
		return Session{}, fmt.Errorf("durableacp: session %q is closed", entry.ID)
	}
	if entry.WorkspaceMode == WorkspaceManaged {
		if _, reopenErr := e.worktrees.Reopen(ctx, entry.Worktree); reopenErr != nil {
			return Session{}, reopenErr
		}
	}
	if _, stateErr := e.runtime.State(entry.ID); stateErr != nil {
		if _, restoreErr := e.runtime.Restore(entry.ID); restoreErr != nil {
			if _, createErr := e.runtime.Create(ctx, runtime.CreateRequest{ID: entry.ID, Backend: entry.Backend, Worktree: entry.Worktree.Path}); createErr != nil {
				return Session{}, errors.Join(restoreErr, createErr)
			}
		}
	}
	configuration := entry.Configuration
	mergeConfiguration(&configuration, configurationFromStart(request))
	resumeBackendSessionID := strings.TrimSpace(request.ResumeBackendSessionID)
	if resumeBackendSessionID == "" {
		resumeBackendSessionID = entry.BackendSession.ID
	}
	state, err := e.runtime.Start(ctx, host.StartSessionRequest{
		SessionID:              entry.ID,
		Prompt:                 request.Prompt,
		Attachments:            request.Attachments,
		Model:                  configuration.Model,
		Reasoning:              configuration.Reasoning,
		PermissionMode:         configuration.PermissionMode,
		ResumeBackendSessionID: resumeBackendSessionID,
		Ext:                    cloneRawMessage(request.Ext),
	})
	if err != nil {
		return Session{}, err
	}
	entry.BackendSession = state
	entry.Configuration = configuration
	if runtimeState, stateErr := e.runtime.State(entry.ID); stateErr == nil {
		entry.Configuration = runtimeState.Configuration
	}
	if len(request.Ext) > 0 {
		entry.Ext = cloneRawMessage(request.Ext)
	}
	if err := e.saveSession(&entry); err != nil {
		return Session{}, err
	}
	return entry, nil
}

// Repair verifies a session workspace after an interrupted process or manual
// Git operation. Managed worktrees are returned to their owned branch;
// existing workspaces are only inspected and never modified.
func (e *Engine) Repair(ctx context.Context, sessionID string) (Session, error) {
	entry, err := e.loadSession(sessionID)
	if err != nil {
		return Session{}, err
	}
	if entry.WorkspaceMode == WorkspaceManaged {
		workspace, err := e.worktrees.Repair(ctx, entry.Worktree)
		if err != nil {
			return Session{}, err
		}
		entry.Worktree = workspace
	} else {
		workspace, err := e.inspectExistingWorkspace(ctx, entry.ID, entry.Worktree.Path)
		if err != nil {
			return Session{}, err
		}
		workspace.Source = entry.Worktree.Source
		entry.Worktree = workspace
	}
	if err := e.saveSession(&entry); err != nil {
		return Session{}, err
	}
	return entry, nil
}

// Prune removes stale Git worktree registrations for source. It deliberately
// does not remove a session directory, manifest, or branch; callers use
// Remove for explicit destructive cleanup.
func (e *Engine) Prune(ctx context.Context, source string) error {
	if e == nil || e.worktrees == nil {
		return errors.New("durableacp: engine is not open")
	}
	return e.worktrees.Prune(ctx, source)
}

// Send forwards or queues a turn for a started session.
func (e *Engine) Send(ctx context.Context, request host.SendTurnRequest) (runtime.SendResult, error) {
	if e == nil || e.runtime == nil {
		return runtime.SendResult{}, errors.New("durableacp: engine is not open")
	}
	return e.runtime.Send(ctx, request)
}

func (e *Engine) SendNext(ctx context.Context, request host.SendTurnRequest) (runtime.SendResult, error) {
	if e == nil || e.runtime == nil {
		return runtime.SendResult{}, errors.New("durableacp: engine is not open")
	}
	return e.runtime.SendNext(ctx, request)
}

// SetConfiguration replaces the effective provider-neutral configuration in
// the session manifest.
func (e *Engine) SetConfiguration(sessionID string, configuration Configuration) error {
	if e == nil || e.runtime == nil {
		return errors.New("durableacp: engine is not open")
	}
	if err := e.runtime.SetConfiguration(sessionID, configuration); err != nil {
		return err
	}
	return e.updateSession(sessionID, func(entry *Session) {
		entry.Configuration = normalizeConfiguration(configuration)
	})
}

// Snapshot returns the complete current Engine state for one session.
func (e *Engine) Snapshot(sessionID string) (Snapshot, error) {
	if e == nil || e.runtime == nil || e.journal == nil {
		return Snapshot{}, errors.New("durableacp: engine is not open")
	}
	entry, err := e.loadSession(strings.TrimSpace(sessionID))
	if err != nil {
		return Snapshot{}, err
	}
	status := session.StatusActive
	updatedAt := entry.UpdatedAt
	backendSession := entry.BackendSession
	runtimeState, runtimeErr := e.runtime.State(entry.ID)
	if runtimeErr == nil {
		status = runtimeState.Session.Status
		if runtimeState.Session.UpdatedAt.After(updatedAt) {
			updatedAt = runtimeState.Session.UpdatedAt
		}
		if runtimeState.Session.BackendSession != (host.BackendSession{}) {
			backendSession = runtimeState.Session.BackendSession
		}
	} else {
		var missing *runtime.SessionNotFoundError
		if !errors.As(runtimeErr, &missing) {
			return Snapshot{}, runtimeErr
		}
	}
	if entry.ClosedAt != nil {
		status = session.StatusClosed
	}
	lastSequence, err := e.journal.LastSequence(entry.ID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, err
	}
	snapshot := Snapshot{
		SessionID:           entry.ID,
		Lifecycle:           LifecycleSnapshot{Status: status, CreatedAt: entry.CreatedAt, UpdatedAt: updatedAt, ClosedAt: entry.ClosedAt},
		Workspace:           WorkspaceSnapshot{Mode: entry.WorkspaceMode, Workspace: entry.Worktree},
		Backend:             BackendSnapshot{Backend: entry.Backend, Session: backendSession},
		Configuration:       entry.Configuration,
		LastJournalSequence: lastSequence,
		Ext:                 cloneRawMessage(entry.Ext),
	}
	if runtimeErr == nil {
		snapshot.Configuration = runtimeState.Configuration
		snapshot.ActiveTurn = ActiveTurnSnapshot{Active: runtimeState.TurnActive, ID: runtimeState.ActiveTurnID}
		snapshot.Queue = QueueSnapshot{Depth: runtimeState.QueueDepth, Entries: runtimeState.QueueEntries, DispatchBlocked: runtimeState.DispatchBlocked}
		snapshot.PendingInteraction = runtimeState.PendingInteraction
	}
	return snapshot, nil
}

func (e *Engine) QueueEntries(sessionID string) ([]runtime.QueueEntry, error) {
	if e == nil || e.runtime == nil {
		return nil, errors.New("durableacp: engine is not open")
	}
	return e.runtime.QueueEntries(sessionID)
}

func (e *Engine) RemoveQueuedTurn(sessionID, entryID string) (runtime.RemoveQueuedTurnResult, error) {
	if e == nil || e.runtime == nil {
		return runtime.RemoveQueuedTurnResult{}, errors.New("durableacp: engine is not open")
	}
	return e.runtime.RemoveQueuedTurn(sessionID, entryID)
}

func (e *Engine) ReplaceQueuedTurn(sessionID, entryID string, request host.SendTurnRequest) (runtime.QueueEntry, error) {
	if e == nil || e.runtime == nil {
		return runtime.QueueEntry{}, errors.New("durableacp: engine is not open")
	}
	return e.runtime.ReplaceQueuedTurn(sessionID, entryID, request)
}

func (e *Engine) BlockDispatch(sessionID string) error {
	if e == nil || e.runtime == nil {
		return errors.New("durableacp: engine is not open")
	}
	return e.runtime.BlockDispatch(sessionID)
}

func (e *Engine) UnblockDispatch(sessionID string) error {
	if e == nil || e.runtime == nil {
		return errors.New("durableacp: engine is not open")
	}
	return e.runtime.UnblockDispatch(sessionID)
}

func (e *Engine) Restart(ctx context.Context, sessionID string) (host.BackendSession, error) {
	if e == nil || e.runtime == nil {
		return host.BackendSession{}, errors.New("durableacp: engine is not open")
	}
	entry, err := e.loadSession(sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	state, err := e.runtime.Restart(ctx, sessionID)
	if err != nil {
		return host.BackendSession{}, err
	}
	entry.BackendSession = state
	if err := e.saveSession(&entry); err != nil {
		return host.BackendSession{}, err
	}
	return state, nil
}

// Interrupt cancels the active turn and clears queued turns.
func (e *Engine) Interrupt(ctx context.Context, sessionID string) error {
	if e == nil || e.runtime == nil {
		return errors.New("durableacp: engine is not open")
	}
	return e.runtime.Interrupt(ctx, sessionID)
}

func (e *Engine) InterruptActive(ctx context.Context, sessionID string) error {
	if e == nil || e.runtime == nil {
		return errors.New("durableacp: engine is not open")
	}
	return e.runtime.InterruptActive(ctx, sessionID)
}

// RespondInteraction resolves a pending adapter request.
func (e *Engine) RespondInteraction(ctx context.Context, sessionID string, response host.InteractionResponse) error {
	if e == nil || e.runtime == nil {
		return errors.New("durableacp: engine is not open")
	}
	return e.runtime.RespondInteraction(ctx, sessionID, response)
}

// ForkPrompt forwards caller-owned fork instructions to an adapter that
// supports them. Durable ACP keeps the instruction payload opaque.
func (e *Engine) ForkPrompt(ctx context.Context, request host.ForkPromptRequest) (host.ForkPromptResponse, error) {
	if e == nil || e.runtime == nil {
		return host.ForkPromptResponse{}, errors.New("durableacp: engine is not open")
	}
	return e.runtime.ForkPrompt(ctx, request)
}

// CloseSession stops the provider while retaining all managed session state.
func (e *Engine) CloseSession(sessionID string) error {
	entry, err := e.loadSession(sessionID)
	if err != nil {
		return err
	}
	if err := e.runtime.Close(entry.ID); err != nil {
		return err
	}
	now := time.Now().UTC()
	entry.ClosedAt = &now
	return e.saveSession(&entry)
}

// Remove permanently removes a closed session's owned worktree and branch.
// Existing-worktree sessions only lose their Engine manifest.
//
//nolint:govet // Each recovery step reports its own error.
func (e *Engine) Remove(ctx context.Context, sessionID string, force bool) error {
	entry, err := e.loadSession(sessionID)
	if err != nil {
		return err
	}
	if entry.ClosedAt == nil {
		if _, err := e.runtime.State(entry.ID); err != nil {
			if _, restoreErr := e.runtime.Restore(entry.ID); restoreErr != nil {
				if _, createErr := e.runtime.Create(ctx, runtime.CreateRequest{ID: entry.ID, Backend: entry.Backend, Worktree: entry.Worktree.Path}); createErr != nil {
					return errors.Join(restoreErr, createErr)
				}
			}
		}
		if err := e.CloseSession(entry.ID); err != nil {
			return err
		}
		entry, err = e.loadSession(entry.ID)
		if err != nil {
			return err
		}
	}
	if entry.WorkspaceMode == WorkspaceManaged {
		if err := e.worktrees.Remove(ctx, entry.Worktree, worktree.RemoveOptions{Force: force}); err != nil {
			return err
		}
	}
	if err := os.Remove(e.sessionPath(entry.ID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("durableacp: remove session manifest: %w", err)
	}
	return e.runtime.Forget(entry.ID)
}

// Sessions returns persisted engine sessions, including sessions not currently
// restored into this process's runtime.
func (e *Engine) Sessions() ([]Session, error) {
	if e == nil {
		return nil, errors.New("durableacp: engine is not open")
	}
	entries, err := os.ReadDir(filepath.Join(e.home, "sessions"))
	if err != nil {
		return nil, fmt.Errorf("durableacp: list session manifests: %w", err)
	}
	out := make([]Session, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		session, err := e.loadSession(id)
		if err == nil {
			out = append(out, session)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// Append adds an opaque extension event to a session journal. Extension names
// must be owner-qualified so they cannot collide with durable core events.
func (e *Engine) Append(sessionID, name string, data any, presentation *journal.Presentation) (journal.Record, error) {
	if e == nil || e.journal == nil {
		return journal.Record{}, errors.New("durableacp: engine is not open")
	}
	if strings.TrimSpace(sessionID) == "" || !strings.Contains(strings.TrimSpace(name), ".") {
		return journal.Record{}, errors.New("durableacp: session ID and owner-qualified event name are required")
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return journal.Record{}, fmt.Errorf("durableacp: encode extension event: %w", err)
	}
	return e.journal.Append(journal.Record{SessionID: sessionID, Event: name, Data: raw, Presentation: presentation})
}

// Journal returns the raw append-only store for advanced integrations.
func (e *Engine) Journal() *journal.Store {
	if e == nil {
		return nil
	}
	return e.journal
}

// Close closes active provider sessions and the journal. It does not remove
// managed worktrees or manifests.
func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		var joined error
		if e.runtime != nil {
			joined = errors.Join(joined, e.runtime.Shutdown())
		}
		if e.journal != nil && e.journalOwned {
			joined = errors.Join(joined, e.journal.Close())
		}
		for _, closer := range e.closers {
			joined = errors.Join(joined, closer.Close())
		}
		e.closeErr = joined
	})
	return e.closeErr
}

func (e *Engine) publish(event host.Event) {
	if engineConfigurationEvent(event.Type) && e.runtime != nil {
		if state, err := e.runtime.State(event.SessionID); err == nil {
			_ = e.updateSession(event.SessionID, func(entry *Session) {
				entry.Configuration = state.Configuration
			})
		}
	}
	e.mu.Lock()
	listeners := make([]host.EventSink, 0, len(e.listeners))
	for _, listener := range e.listeners {
		listeners = append(listeners, listener)
	}
	e.mu.Unlock()
	for _, listener := range listeners {
		listener(event)
	}
}

func engineConfigurationEvent(eventType host.EventType) bool {
	//exhaustive:ignore Only generic configuration events are persisted here.
	switch eventType {
	case host.EventModels, host.EventReasoningLevels, host.EventPermissionModes:
		return true
	default:
		return false
	}
}

func (e *Engine) rollbackStart(ctx context.Context, entry Session) {
	_ = e.runtime.Close(entry.ID)
	_ = e.runtime.Forget(entry.ID)
	_ = os.Remove(e.sessionPath(entry.ID))
	if entry.WorkspaceMode == WorkspaceManaged {
		_ = e.worktrees.Remove(ctx, entry.Worktree, worktree.RemoveOptions{Force: true})
	}
}

func (e *Engine) sessionPath(id string) string {
	return filepath.Join(e.home, "sessions", safeFileName(id)+".json")
}

func (e *Engine) inspectExistingWorkspace(ctx context.Context, id, path string) (worktree.Session, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return worktree.Session{}, errors.New("durableacp: existing workspace is required")
	}
	if !filepath.IsAbs(path) {
		return worktree.Session{}, errors.New("durableacp: existing workspace must be absolute")
	}
	path = filepath.Clean(path)
	info, err := os.Stat(path)
	if err != nil {
		return worktree.Session{}, fmt.Errorf("durableacp: inspect existing workspace: %w", err)
	}
	if !info.IsDir() {
		return worktree.Session{}, fmt.Errorf("durableacp: existing workspace %q is not a directory", path)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(path); resolveErr == nil {
		path = resolved
	}
	workspace, inspectErr := e.worktrees.Inspect(ctx, path)
	if inspectErr != nil {
		workspace = worktree.Session{Path: path}
	}
	workspace.ID = id
	workspace.Source = path
	return workspace, nil
}

func (e *Engine) saveSession(entry *Session) error {
	if entry == nil {
		return errors.New("durableacp: nil session manifest")
	}
	entry.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("durableacp: encode session manifest: %w", err)
	}
	return writePrivate(e.sessionPath(entry.ID), raw)
}

func (e *Engine) loadSession(id string) (Session, error) {
	if !validSessionID(id) {
		return Session{}, fmt.Errorf("durableacp: invalid session ID %q", id)
	}
	path := e.sessionPath(id)
	// #nosec G304 -- sessionPath validates the ID and derives the path below Engine home.
	raw, err := os.ReadFile(path)
	if err != nil {
		return Session{}, fmt.Errorf("durableacp: read session manifest: %w", err)
	}
	var entry Session
	if err := json.Unmarshal(raw, &entry); err != nil {
		return Session{}, fmt.Errorf("durableacp: decode session manifest: %w", err)
	}
	if entry.ID == "" {
		return Session{}, errors.New("durableacp: invalid session manifest")
	}
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = entry.CreatedAt
	}
	entry.Configuration = normalizeConfiguration(entry.Configuration)
	entry.Ext = cloneRawMessage(entry.Ext)
	return entry, nil
}

func (e *Engine) recordTurnSubmission(submission runtime.TurnSubmission) {
	_ = e.updateSession(submission.SessionID, func(entry *Session) {
		entry.BackendSession = submission.Session
		if runtimeState, err := e.runtime.State(submission.SessionID); err == nil {
			entry.Configuration = runtimeState.Configuration
			return
		}
		mergeConfiguration(&entry.Configuration, Configuration{Model: submission.Request.Model, Reasoning: submission.Request.Reasoning, PermissionMode: submission.Request.PermissionMode})
	})
}

func (e *Engine) updateSession(sessionID string, update func(*Session)) error {
	e.manifestMu.Lock()
	defer e.manifestMu.Unlock()
	entry, err := e.loadSession(sessionID)
	if err != nil {
		return err
	}
	if update != nil {
		update(&entry)
	}
	return e.saveSession(&entry)
}

func configurationFromStart(request StartRequest) Configuration {
	return normalizeConfiguration(Configuration{Model: request.Model, Reasoning: request.Reasoning, PermissionMode: request.PermissionMode})
}

func normalizeConfiguration(configuration Configuration) Configuration {
	configuration.Model = strings.TrimSpace(configuration.Model)
	configuration.Reasoning = strings.TrimSpace(configuration.Reasoning)
	configuration.PermissionMode = strings.TrimSpace(configuration.PermissionMode)
	return configuration
}

func mergeConfiguration(target *Configuration, update Configuration) {
	update = normalizeConfiguration(update)
	if update.Model != "" {
		target.Model = update.Model
	}
	if update.Reasoning != "" {
		target.Reasoning = update.Reasoning
	}
	if update.PermissionMode != "" {
		target.PermissionMode = update.PermissionMode
	}
	*target = normalizeConfiguration(*target)
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func loadSettings(home string) (Settings, error) {
	path := filepath.Join(home, configFileName)
	// #nosec G304 -- config path is derived from the Engine home selected by the embedding host.
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return DefaultSettings(), nil
	}
	if err != nil {
		return Settings{}, fmt.Errorf("durableacp: read config: %w", err)
	}
	settings := DefaultSettings()
	if err := json.Unmarshal(raw, &settings); err != nil {
		return Settings{}, fmt.Errorf("durableacp: decode config: %w", err)
	}
	return settings, normalizeSettings(&settings)
}

func normalizeSettings(settings *Settings) error {
	if settings == nil {
		return errors.New("durableacp: nil settings")
	}
	if settings.Version == 0 {
		settings.Version = 1
	}
	if settings.Version != 1 {
		return fmt.Errorf("durableacp: unsupported config version %d", settings.Version)
	}
	settings.Worktrees.BranchPrefix = strings.Trim(strings.TrimSpace(settings.Worktrees.BranchPrefix), "/")
	if settings.Worktrees.BranchPrefix == "" {
		settings.Worktrees.BranchPrefix = DefaultSettings().Worktrees.BranchPrefix
	}
	return nil
}

func saveSettings(home string, settings Settings) error {
	raw, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("durableacp: encode config: %w", err)
	}
	raw = append(raw, '\n')
	return writePrivate(filepath.Join(home, configFileName), raw)
}

func catalogDir(home string, settings Settings) string {
	if !settings.CatalogCache {
		return ""
	}
	return filepath.Join(home, "cache")
}

func secureDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	// #nosec G302 -- Engine directories are intentionally private to their owner.
	return os.Chmod(path, 0o700)
}

func writePrivate(path string, raw []byte) error {
	if err := secureDir(filepath.Dir(path)); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temporary.Name()) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporary.Name(), path)
}

func openProviderLogs(home string) (map[host.Backend]io.Writer, []io.Closer, error) {
	logs := map[host.Backend]io.Writer{}
	closers := make([]io.Closer, 0, 4)
	for _, backend := range []host.Backend{"claude", "codex", "cursor", "antigravity"} {
		path := filepath.Join(home, "logs", "providers", string(backend)+".log")
		// #nosec G304 -- path is constructed below Engine home from a fixed backend name.
		file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			for _, closer := range closers {
				_ = closer.Close()
			}
			return nil, nil, fmt.Errorf("durableacp: open %s: %w", path, err)
		}
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			for _, closer := range closers {
				_ = closer.Close()
			}
			return nil, nil, fmt.Errorf("durableacp: secure %s: %w", path, err)
		}
		logs[backend] = file
		closers = append(closers, file)
	}
	return logs, closers, nil
}

func mergeAdapters(adapters []host.Adapter) []host.Adapter {
	byBackend := map[host.Backend]host.Adapter{}
	for _, adapter := range adapters {
		if adapter != nil && adapter.Backend() != "" {
			byBackend[adapter.Backend()] = adapter
		}
	}
	out := make([]host.Adapter, 0, len(byBackend))
	for _, adapter := range byBackend {
		out = append(out, adapter)
	}
	return out
}

func safeFileName(value string) string {
	var out strings.Builder
	for _, character := range strings.TrimSpace(value) {
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z', character >= '0' && character <= '9', character == '-', character == '_':
			out.WriteRune(character)
		default:
			out.WriteByte('-')
		}
	}
	if out.Len() == 0 {
		return "session"
	}
	return out.String()
}

func validSessionID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return false
	}
	return safeFileName(value) == value
}

func sessionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("durableacp: create session ID: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
