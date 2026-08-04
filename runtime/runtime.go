// Package runtime multiplexes provider-neutral ACP adapters into durable host
// sessions. It deliberately accepts adapters from its caller rather than
// importing provider implementations.
package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/meloniteai/durable-acp/host"
	"github.com/meloniteai/durable-acp/journal"
	"github.com/meloniteai/durable-acp/session"
)

const (
	defaultQueueDepth       = 16
	defaultCoalesceInterval = 24 * time.Millisecond
	defaultCatalogTimeout   = 45 * time.Second
)

// Config configures a Runtime. Journal and EventSink are optional, allowing
// the package to be used as an in-memory multiplexer in tests or short-lived
// applications.
type Config struct {
	Journal           *journal.Store
	EventSink         host.EventSink
	TurnDispatched    func(TurnDispatch) error
	TurnSubmitted     func(TurnSubmission)
	MaxQueuedTurns    int
	CatalogCacheDir   string
	CatalogTimeout    time.Duration
	CatalogUpdated    func(host.Backend, host.BackendCatalog)
	CoalesceInterval  time.Duration
	DisableCoalescing bool
}

// CreateRequest creates a session before its provider process is started.
type CreateRequest struct {
	ID       string
	Backend  host.Backend
	Worktree string
}

// State is the current runtime-owned view of a session.
type State struct {
	Session            session.Session           `json:"session"`
	Configuration      host.SessionConfiguration `json:"configuration"`
	QueueDepth         int                       `json:"queue_depth"`
	QueueEntries       []QueueEntry              `json:"queue_entries,omitempty"`
	TurnActive         bool                      `json:"turn_active"`
	ActiveTurnID       string                    `json:"active_turn_id,omitempty"`
	DispatchBlocked    bool                      `json:"dispatch_blocked,omitempty"`
	PendingInteraction *host.InteractionRequest  `json:"pending_interaction,omitempty"`
}

type QueueEntry struct {
	ID      string               `json:"id"`
	Request host.SendTurnRequest `json:"request"`
}

type RemoveQueuedTurnResult struct {
	Removed    bool       `json:"removed"`
	Entry      QueueEntry `json:"entry,omitzero"`
	QueueDepth int        `json:"queue_depth"`
}

type TurnDispatch struct {
	SessionID    string               `json:"session_id"`
	QueueEntryID string               `json:"queue_entry_id,omitempty"`
	Request      host.SendTurnRequest `json:"request"`
	Queued       bool                 `json:"queued"`
}

type TurnSubmission struct {
	TurnDispatch
	Session host.BackendSession `json:"session"`
}

// SendResult reports whether a turn was started or placed behind the current
// provider turn.
type SendResult struct {
	Session      host.BackendSession `json:"session"`
	Accepted     bool                `json:"accepted"`
	Queued       bool                `json:"queued,omitempty"`
	QueueEntryID string              `json:"queue_entry_id,omitempty"`
	QueueDepth   int                 `json:"queue_depth,omitempty"`
}

// CatalogProvider is optional adapter capability used for startup model
// discovery and cache refresh.
type CatalogProvider interface {
	Catalog(context.Context) (host.BackendCatalog, error)
}

// CatalogResult records one provider's outcome during a catalog refresh.
// Status is pass, skip, or fail. A skipped provider is not currently
// available or does not advertise catalog discovery.
type CatalogResult struct {
	Backend host.Backend `json:"backend"`
	Status  string       `json:"status"`
	Detail  string       `json:"detail,omitempty"`
}

// Runtime owns adapters, sessions, turn queues, and event normalization.
type Runtime struct {
	mu               sync.Mutex
	adapters         map[host.Backend]host.Adapter
	sessions         map[string]*managedSession
	journal          *journal.Store
	emit             host.EventSink
	turnDispatched   func(TurnDispatch) error
	turnSubmitted    func(TurnSubmission)
	maxQueuedTurns   int
	coalesceInterval time.Duration
	coalesceEvents   bool
	catalogCacheDir  string
	catalogTimeout   time.Duration
	catalogUpdated   func(host.Backend, host.BackendCatalog)
	catalogSaveMu    sync.Mutex
	catalog          map[host.Backend]host.BackendCatalog
}

type managedSession struct {
	session            session.Session
	configuration      host.SessionConfiguration
	configurationEvent configurationEventVersions
	queue              []QueueEntry
	dispatching        bool
	active             bool
	activeTurnID       string
	dispatchBlocked    bool
	pendingInteraction *host.InteractionRequest
	nextQueueSeq       int
	nextSeq            int
	coalescer          *deltaCoalescer
}

type configurationEventVersions struct {
	model      uint64
	reasoning  uint64
	permission uint64
}

// New creates a Runtime over the supplied adapters. Duplicate backend names
// are resolved in favor of the final adapter.
func New(config Config, adapters ...host.Adapter) *Runtime {
	limit := config.MaxQueuedTurns
	if limit <= 0 {
		limit = defaultQueueDepth
	}
	interval := config.CoalesceInterval
	if interval <= 0 {
		interval = defaultCoalesceInterval
	}
	catalogTimeout := config.CatalogTimeout
	if catalogTimeout <= 0 {
		catalogTimeout = defaultCatalogTimeout
	}
	runtime := &Runtime{
		adapters:         map[host.Backend]host.Adapter{},
		sessions:         map[string]*managedSession{},
		journal:          config.Journal,
		emit:             config.EventSink,
		turnDispatched:   config.TurnDispatched,
		turnSubmitted:    config.TurnSubmitted,
		maxQueuedTurns:   limit,
		coalesceInterval: interval,
		coalesceEvents:   !config.DisableCoalescing,
		catalogCacheDir:  strings.TrimSpace(config.CatalogCacheDir),
		catalogTimeout:   catalogTimeout,
		catalogUpdated:   config.CatalogUpdated,
		catalog:          map[host.Backend]host.BackendCatalog{},
	}
	for _, adapter := range adapters {
		if adapter != nil && strings.TrimSpace(string(adapter.Backend())) != "" {
			runtime.adapters[adapter.Backend()] = adapter
		}
	}
	return runtime
}

// Create registers a session and persists its durable identity before an
// adapter process is launched.
func (r *Runtime) Create(_ context.Context, request CreateRequest) (State, error) {
	if r == nil {
		return State{}, errors.New("runtime: nil runtime")
	}
	backend := host.Backend(strings.TrimSpace(string(request.Backend)))
	if backend == "" {
		return State{}, errors.New("runtime: backend is required")
	}
	worktree := strings.TrimSpace(request.Worktree)
	if worktree == "" {
		return State{}, errors.New("runtime: worktree is required")
	}
	worktree = filepath.Clean(worktree)
	if _, err := os.Stat(worktree); err != nil {
		return State{}, fmt.Errorf("runtime: worktree %q: %w", worktree, err)
	}
	r.mu.Lock()
	if _, ok := r.adapters[backend]; !ok {
		r.mu.Unlock()
		return State{}, fmt.Errorf("runtime: unsupported backend %q", backend)
	}
	id := strings.TrimSpace(request.ID)
	if id == "" {
		var err error
		id, err = newID()
		if err != nil {
			r.mu.Unlock()
			return State{}, err
		}
	}
	if _, exists := r.sessions[id]; exists {
		r.mu.Unlock()
		return State{}, &SessionExistsError{SessionID: id}
	}
	created := session.New(id, backend, worktree)
	if err := created.Transition(session.StatusActive); err != nil {
		r.mu.Unlock()
		return State{}, err
	}
	managed := &managedSession{session: *created}
	r.sessions[id] = managed
	r.mu.Unlock()
	if err := r.appendLifecycle(id, "session.created", map[string]any{
		"backend":  backend,
		"worktree": worktree,
	}); err != nil {
		r.mu.Lock()
		delete(r.sessions, id)
		r.mu.Unlock()
		return State{}, err
	}
	return r.State(id)
}

// Start launches or resumes the adapter session registered by Create.
func (r *Runtime) Start(ctx context.Context, request host.StartSessionRequest) (host.BackendSession, error) {
	if r == nil {
		return host.BackendSession{}, errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(request.SessionID)
	if id == "" {
		return host.BackendSession{}, errors.New("runtime: session ID is required")
	}
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(id)
	if err != nil {
		r.mu.Unlock()
		return host.BackendSession{}, err
	}
	if managed.session.Status == session.StatusClosed {
		r.mu.Unlock()
		return host.BackendSession{}, &SessionClosedError{SessionID: id}
	}
	request.SessionID = id
	request.Backend = managed.session.Backend
	request.Worktree = managed.session.Worktree
	previousConfiguration := managed.configuration
	managed.configuration = mergeConfiguration(managed.configuration, configurationFromStart(request))
	request = fillStartConfiguration(request, managed.configuration)
	initialTurn := strings.TrimSpace(request.Prompt) != "" || len(request.Attachments) > 0
	if initialTurn {
		managed.dispatching = true
	}
	r.mu.Unlock()

	state, err := adapter.StartSession(ctx, id, request, r.sessionSink(id)) //nolint:contextcheck // Adapter callbacks may dispatch queued work after the request returns.
	if err != nil {
		r.mu.Lock()
		if current := r.sessions[id]; current != nil {
			current.dispatching = false
			current.configuration = previousConfiguration
		}
		r.mu.Unlock()
		return host.BackendSession{}, err
	}
	r.mu.Lock()
	if current := r.sessions[id]; current != nil {
		current.session.BackendSession = state
	}
	r.mu.Unlock()
	if err := r.appendLifecycle(id, "session.started", map[string]any{
		"backend":         request.Backend,
		"backend_session": state,
		"configuration":   managedConfiguration(r, id),
	}); err != nil {
		return host.BackendSession{}, err
	}
	return state, nil
}

// Send starts a turn immediately when idle, otherwise queues it behind the
// current provider turn.
func (r *Runtime) Send(ctx context.Context, request host.SendTurnRequest) (SendResult, error) {
	return r.send(ctx, request, false)
}

func (r *Runtime) SendNext(ctx context.Context, request host.SendTurnRequest) (SendResult, error) {
	return r.send(ctx, request, true)
}

func (r *Runtime) send(ctx context.Context, request host.SendTurnRequest, next bool) (SendResult, error) {
	if r == nil {
		return SendResult{}, errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(request.SessionID)
	if id == "" {
		return SendResult{}, errors.New("runtime: session ID is required")
	}
	if strings.TrimSpace(request.Prompt) == "" && len(request.Attachments) == 0 {
		return SendResult{}, errors.New("runtime: prompt or attachment is required")
	}
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(id)
	if err != nil {
		r.mu.Unlock()
		return SendResult{}, err
	}
	if managed.session.Status == session.StatusClosed {
		r.mu.Unlock()
		return SendResult{}, &SessionClosedError{SessionID: id}
	}
	request.SessionID = id
	request = fillTurnConfiguration(request, managed.configuration)
	request = cloneSendTurnRequest(request)
	if managed.active || managed.dispatching || managed.dispatchBlocked {
		if len(managed.queue) >= r.maxQueuedTurns {
			depth := len(managed.queue)
			r.mu.Unlock()
			return SendResult{}, &QueueFullError{SessionID: id, Depth: depth, Limit: r.maxQueuedTurns}
		}
		managed.nextQueueSeq++
		entry := QueueEntry{ID: fmt.Sprintf("%s:q:%d", id, managed.nextQueueSeq), Request: request}
		if next {
			managed.queue = append([]QueueEntry{entry}, managed.queue...)
		} else {
			managed.queue = append(managed.queue, entry)
		}
		depth := len(managed.queue)
		r.mu.Unlock()
		r.emitQueue(id)
		return SendResult{Accepted: true, Queued: true, QueueEntryID: entry.ID, QueueDepth: depth}, nil
	}
	managed.dispatching = true
	managed.active = false
	managed.activeTurnID = ""
	r.mu.Unlock()

	dispatch := TurnDispatch{SessionID: id, Request: request}
	state, err := r.submitTurn(ctx, id, adapter, dispatch)
	if err != nil {
		return SendResult{}, err
	}
	r.emitQueue(id)
	return SendResult{Session: state, Accepted: true}, nil
}

// Interrupt cancels the active provider turn and discards queued turns.
func (r *Runtime) Interrupt(ctx context.Context, sessionID string) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(id)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	active := managed.active
	managed.queue = nil
	r.mu.Unlock()
	if active {
		if err := r.interruptActive(ctx, id, adapter); err != nil {
			return err
		}
	}
	r.emitQueue(id)
	return nil
}

func (r *Runtime) InterruptActive(ctx context.Context, sessionID string) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(id)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	active := managed.active
	r.mu.Unlock()
	if !active {
		return nil
	}
	return r.interruptActive(ctx, id, adapter)
}

func (r *Runtime) ReconcileTurn(sessionID string, event host.Event) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	if event.Type != host.EventTurnComplete && event.Type != host.EventTurnFailed && event.Type != host.EventProcessExited {
		return errors.New("runtime: terminal turn event is required")
	}
	r.mu.Lock()
	managed := r.sessions[id]
	r.mu.Unlock()
	if managed == nil {
		return &SessionNotFoundError{SessionID: id}
	}
	r.deliver(id, event)
	return nil
}

// RespondInteraction sends a normalized user response to the session adapter.
func (r *Runtime) RespondInteraction(ctx context.Context, sessionID string, response host.InteractionResponse) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	if strings.TrimSpace(response.RequestID) == "" {
		return errors.New("runtime: interaction request ID is required")
	}
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(strings.TrimSpace(sessionID))
	if err == nil && managed.session.Status == session.StatusWaitingInput {
		_ = managed.session.Transition(session.StatusRunning)
	}
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if responder, ok := adapter.(host.InteractionResponder); ok {
		err = responder.RespondInteraction(ctx, sessionID, response)
	} else if responder, ok := adapter.(host.PermissionResponder); ok {
		allow := response.Action == "approve" || response.Action == "allow"
		err = responder.RespondPermission(ctx, sessionID, response.RequestID, allow, response.Message, "")
	} else {
		return fmt.Errorf("runtime: backend %q does not support interaction responses", adapter.Backend())
	}
	if err != nil {
		return err
	}
	r.deliver(strings.TrimSpace(sessionID), host.Event{Type: host.EventInteractionResolved, InteractionResponse: &response}) //nolint:contextcheck // Event delivery may outlive the request callback.
	return nil
}

// ForkPrompt delegates a caller-owned fork instruction to adapters that
// explicitly advertise host.SessionForker. Durable runtime does not interpret
// the prompt, instructions, or MCP server configuration.
func (r *Runtime) ForkPrompt(ctx context.Context, request host.ForkPromptRequest) (host.ForkPromptResponse, error) {
	if r == nil {
		return host.ForkPromptResponse{}, errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(request.SessionID)
	if id == "" {
		return host.ForkPromptResponse{}, errors.New("runtime: session ID is required")
	}
	r.mu.Lock()
	_, adapter, err := r.adapterLocked(id)
	r.mu.Unlock()
	if err != nil {
		return host.ForkPromptResponse{}, err
	}
	forker, ok := adapter.(host.SessionForker)
	if !ok {
		return host.ForkPromptResponse{}, fmt.Errorf("runtime: backend %q does not support session forks", adapter.Backend())
	}
	request.SessionID = id
	return forker.ForkPrompt(ctx, request)
}

func (r *Runtime) QueueEntries(sessionID string) ([]QueueEntry, error) {
	if r == nil {
		return nil, errors.New("runtime: nil runtime")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	managed := r.sessions[strings.TrimSpace(sessionID)]
	if managed == nil {
		return nil, &SessionNotFoundError{SessionID: strings.TrimSpace(sessionID)}
	}
	return cloneQueueEntries(managed.queue), nil
}

func (r *Runtime) RemoveQueuedTurn(sessionID, entryID string) (RemoveQueuedTurnResult, error) {
	if r == nil {
		return RemoveQueuedTurnResult{}, errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	queuedID := strings.TrimSpace(entryID)
	r.mu.Lock()
	managed := r.sessions[id]
	if managed == nil {
		r.mu.Unlock()
		return RemoveQueuedTurnResult{}, &SessionNotFoundError{SessionID: id}
	}
	index := -1
	for i := range managed.queue {
		if managed.queue[i].ID == queuedID {
			index = i
			break
		}
	}
	if index < 0 {
		depth := len(managed.queue)
		r.mu.Unlock()
		return RemoveQueuedTurnResult{QueueDepth: depth}, nil
	}
	entry := cloneQueueEntry(managed.queue[index])
	managed.queue = append(append([]QueueEntry(nil), managed.queue[:index]...), managed.queue[index+1:]...)
	depth := len(managed.queue)
	r.mu.Unlock()
	r.emitQueue(id)
	return RemoveQueuedTurnResult{Removed: true, Entry: entry, QueueDepth: depth}, nil
}

func (r *Runtime) ReplaceQueuedTurn(sessionID, entryID string, request host.SendTurnRequest) (QueueEntry, error) {
	if r == nil {
		return QueueEntry{}, errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	queuedID := strings.TrimSpace(entryID)
	if strings.TrimSpace(request.Prompt) == "" && len(request.Attachments) == 0 {
		return QueueEntry{}, errors.New("runtime: prompt or attachment is required")
	}
	r.mu.Lock()
	managed := r.sessions[id]
	if managed == nil {
		r.mu.Unlock()
		return QueueEntry{}, &SessionNotFoundError{SessionID: id}
	}
	for i := range managed.queue {
		if managed.queue[i].ID != queuedID {
			continue
		}
		request.SessionID = id
		request = fillTurnConfiguration(request, managed.configuration)
		managed.queue[i].Request = cloneSendTurnRequest(request)
		entry := cloneQueueEntry(managed.queue[i])
		r.mu.Unlock()
		r.emitQueue(id)
		return entry, nil
	}
	r.mu.Unlock()
	return QueueEntry{}, &QueueEntryNotFoundError{SessionID: id, EntryID: queuedID}
}

func (r *Runtime) BlockDispatch(sessionID string) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed := r.sessions[id]
	if managed == nil {
		r.mu.Unlock()
		return &SessionNotFoundError{SessionID: id}
	}
	managed.dispatchBlocked = true
	r.mu.Unlock()
	r.emitQueue(id)
	return nil
}

func (r *Runtime) UnblockDispatch(sessionID string) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed := r.sessions[id]
	if managed == nil {
		r.mu.Unlock()
		return &SessionNotFoundError{SessionID: id}
	}
	managed.dispatchBlocked = false
	dispatch := !managed.active && !managed.dispatching && len(managed.queue) > 0 && managed.session.Status != session.StatusClosed
	r.mu.Unlock()
	r.emitQueue(id)
	if dispatch {
		go r.dispatchQueued(id)
	}
	return nil
}

func (r *Runtime) Restart(ctx context.Context, sessionID string) (host.BackendSession, error) {
	if r == nil {
		return host.BackendSession{}, errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(id)
	if err != nil {
		r.mu.Unlock()
		return host.BackendSession{}, err
	}
	if managed.session.Status == session.StatusClosed {
		r.mu.Unlock()
		return host.BackendSession{}, &SessionClosedError{SessionID: id}
	}
	if managed.active || managed.dispatching {
		turnID := managed.activeTurnID
		r.mu.Unlock()
		return host.BackendSession{}, &TurnActiveError{SessionID: id, TurnID: turnID}
	}
	restarter, ok := adapter.(host.AgentRestarter)
	if !ok {
		backend := string(adapter.Backend())
		r.mu.Unlock()
		return host.BackendSession{}, &RestartUnsupportedError{Backend: backend}
	}
	r.mu.Unlock()
	state, err := restarter.RestartSession(ctx, id, r.sessionSink(id)) //nolint:contextcheck // Adapter callbacks may dispatch queued work after the request returns.
	if err != nil {
		return host.BackendSession{}, err
	}
	r.mu.Lock()
	if current := r.sessions[id]; current != nil {
		current.session.BackendSession = state
		current.dispatching = false
		current.activeTurnID = ""
	}
	r.mu.Unlock()
	if err := r.appendLifecycle(id, "session.restarted", map[string]any{"backend_session": state}); err != nil {
		return host.BackendSession{}, err
	}
	return state, nil
}

// Close closes the provider session while retaining its durable records.
func (r *Runtime) Close(sessionID string) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(id)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	if managed.session.Status == session.StatusClosed {
		r.mu.Unlock()
		return nil
	}
	coalescer := managed.coalescer
	r.mu.Unlock()
	if coalescer != nil {
		coalescer.Close()
	}
	if err := adapter.CloseSession(id); err != nil {
		return err
	}
	r.mu.Lock()
	if current := r.sessions[id]; current != nil && current.session.Status != session.StatusClosed {
		_ = current.session.Transition(session.StatusClosed)
		current.dispatching = false
		current.active = false
		current.activeTurnID = ""
		current.queue = nil
	}
	r.mu.Unlock()
	if err := r.appendLifecycle(id, "session.closed", nil); err != nil {
		return err
	}
	r.emitQueue(id)
	return nil
}

// Forget removes a closed session after its durable owner has removed or
// rolled back the corresponding identity.
func (r *Runtime) Forget(sessionID string) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed := r.sessions[id]
	if managed == nil {
		r.mu.Unlock()
		return nil
	}
	if managed.session.Status != session.StatusClosed {
		r.mu.Unlock()
		return fmt.Errorf("runtime: session %q must be closed before it is forgotten", id)
	}
	delete(r.sessions, id)
	coalescer := managed.coalescer
	r.mu.Unlock()
	if coalescer != nil {
		coalescer.Close()
	}
	return nil
}

// Shutdown closes provider processes without changing durable session state.
// It is intended for an embedding application's own shutdown path: sessions
// remain resumable when the next Engine opens the same home directory.
// Individual logical sessions must be closed with Close.
func (r *Runtime) Shutdown() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	type closing struct {
		id      string
		adapter host.Adapter
	}
	closings := make([]closing, 0, len(r.sessions))
	coalescers := make([]*deltaCoalescer, 0, len(r.sessions))
	for id, managed := range r.sessions {
		if managed.coalescer != nil {
			coalescers = append(coalescers, managed.coalescer)
		}
		if managed.session.Status == session.StatusClosed {
			continue
		}
		if adapter := r.adapters[managed.session.Backend]; adapter != nil {
			closings = append(closings, closing{id: id, adapter: adapter})
		}
	}
	r.mu.Unlock()
	for _, coalescer := range coalescers {
		coalescer.Close()
	}

	var joined error
	for _, closing := range closings {
		if err := closing.adapter.CloseSession(closing.id); err != nil {
			joined = errors.Join(joined, fmt.Errorf("runtime: shut down session %q: %w", closing.id, err))
		}
	}
	return joined
}

// State returns the current in-memory state for one session.
func (r *Runtime) State(sessionID string) (State, error) {
	if r == nil {
		return State{}, errors.New("runtime: nil runtime")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	managed := r.sessions[strings.TrimSpace(sessionID)]
	if managed == nil {
		return State{}, &SessionNotFoundError{SessionID: strings.TrimSpace(sessionID)}
	}
	return stateLocked(managed), nil
}

func (r *Runtime) SetConfiguration(sessionID string, configuration host.SessionConfiguration) error {
	if r == nil {
		return errors.New("runtime: nil runtime")
	}
	id := strings.TrimSpace(sessionID)
	r.mu.Lock()
	managed := r.sessions[id]
	if managed == nil {
		r.mu.Unlock()
		return &SessionNotFoundError{SessionID: id}
	}
	managed.configuration = normalizeConfiguration(configuration)
	r.mu.Unlock()
	return nil
}

// Sessions returns stable snapshots ordered by creation time then ID.
func (r *Runtime) Sessions() []State {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	states := make([]State, 0, len(r.sessions))
	for _, managed := range r.sessions {
		states = append(states, stateLocked(managed))
	}
	r.mu.Unlock()
	sort.Slice(states, func(i, j int) bool {
		if states[i].Session.CreatedAt.Equal(states[j].Session.CreatedAt) {
			return states[i].Session.ID < states[j].Session.ID
		}
		return states[i].Session.CreatedAt.Before(states[j].Session.CreatedAt)
	})
	return states
}

// Restore reconstructs one session identity from its journal. It does not
// start an adapter or resume a provider session.
func (r *Runtime) Restore(sessionID string) (State, error) {
	if r == nil || r.journal == nil {
		return State{}, errors.New("runtime: journal is required to restore a session")
	}
	id := strings.TrimSpace(sessionID)
	records, err := r.journal.Read(id, 0)
	if err != nil {
		return State{}, err
	}
	var restored *managedSession
	for _, record := range records {
		switch record.Event {
		case "session.created":
			var data struct {
				Backend  host.Backend `json:"backend"`
				Worktree string       `json:"worktree"`
			}
			if err := json.Unmarshal(record.Data, &data); err != nil || data.Backend == "" || data.Worktree == "" {
				continue
			}
			created := session.New(id, data.Backend, data.Worktree)
			created.CreatedAt = record.Timestamp
			created.UpdatedAt = record.Timestamp
			_ = created.Transition(session.StatusActive)
			restored = &managedSession{session: *created}
		case "session.started":
			if restored == nil {
				continue
			}
			var data struct {
				BackendSession host.BackendSession       `json:"backend_session"`
				Configuration  host.SessionConfiguration `json:"configuration"`
			}
			if json.Unmarshal(record.Data, &data) == nil {
				restored.session.BackendSession = data.BackendSession
				restored.configuration = normalizeConfiguration(data.Configuration)
			}
		case "session.closed":
			if restored != nil && restored.session.Status != session.StatusClosed {
				_ = restored.session.Transition(session.StatusClosed)
			}
		}
	}
	if restored == nil {
		return State{}, fmt.Errorf("runtime: no session lifecycle found for %q", id)
	}
	r.mu.Lock()
	if existing := r.sessions[id]; existing != nil {
		restored = existing
	} else {
		r.sessions[id] = restored
	}
	r.mu.Unlock()
	return stateLocked(restored), nil
}

// Detect returns every registered backend in deterministic order.
func (r *Runtime) Detect(ctx context.Context) []host.BackendStatus {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	adapters := make([]host.Adapter, 0, len(r.adapters))
	for _, adapter := range r.adapters {
		adapters = append(adapters, adapter)
	}
	r.mu.Unlock()
	statuses := make([]host.BackendStatus, 0, len(adapters))
	for _, adapter := range adapters {
		statuses = append(statuses, adapter.Detect(ctx))
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Backend < statuses[j].Backend })
	return statuses
}

// Catalog returns cached provider catalogs, refreshing providers when refresh
// is true. Failed providers leave their last successfully cached value intact.
func (r *Runtime) Catalog(ctx context.Context, refresh bool) map[host.Backend]host.BackendCatalog {
	if r == nil {
		return nil
	}
	r.loadCatalogCache()
	if refresh {
		r.RefreshCatalog(ctx)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneCatalog(r.catalog)
}

// RefreshCatalog probes all registered catalog providers concurrently. Each
// probe receives its own timeout, so an unavailable agent cannot delay the
// rest of a host's model picker. Successful discoveries update the cache and
// invoke Config.CatalogUpdated as soon as they are available.
func (r *Runtime) RefreshCatalog(ctx context.Context) []CatalogResult {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	adapters := make([]host.Adapter, 0, len(r.adapters))
	for _, adapter := range r.adapters {
		adapters = append(adapters, adapter)
	}
	timeout := r.catalogTimeout
	r.mu.Unlock()

	results := make(chan CatalogResult, len(adapters))
	var group sync.WaitGroup
	for _, adapter := range adapters {
		group.Add(1)
		go func(adapter host.Adapter) {
			defer group.Done()
			results <- r.refreshCatalogProvider(ctx, timeout, adapter)
		}(adapter)
	}
	group.Wait()
	close(results)

	out := make([]CatalogResult, 0, len(adapters))
	for result := range results {
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Backend < out[j].Backend })
	return out
}

// SetCatalog replaces one provider's cached catalog. It is useful when an
// already-running agent later advertises capabilities such as slash commands.
// The supplied value is copied before storage and callbacks never run under
// the Runtime lock.
func (r *Runtime) SetCatalog(backend host.Backend, catalog host.BackendCatalog) {
	if r == nil || strings.TrimSpace(string(backend)) == "" {
		return
	}
	r.setCatalog(backend, catalog)
	r.saveCatalogCache()
	r.notifyCatalog(backend, catalog)
}

func (r *Runtime) refreshCatalogProvider(ctx context.Context, timeout time.Duration, adapter host.Adapter) CatalogResult {
	result := CatalogResult{Backend: adapter.Backend()}
	provider, ok := adapter.(CatalogProvider)
	if !ok {
		result.Status = "skip"
		result.Detail = "model discovery is unavailable"
		return result
	}
	probe, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	status := adapter.Detect(probe)
	if !status.Available {
		result.Status = "skip"
		result.Detail = strings.TrimSpace(status.Error)
		if result.Detail == "" {
			result.Detail = "agent is unavailable"
		}
		return result
	}
	catalog, err := provider.Catalog(probe)
	if err != nil {
		result.Status = "fail"
		result.Detail = err.Error()
		return result
	}
	if emptyCatalog(catalog) {
		result.Status = "fail"
		result.Detail = "agent reported no model configuration"
		return result
	}
	r.setCatalog(result.Backend, catalog)
	r.saveCatalogCache()
	r.notifyCatalog(result.Backend, catalog)
	result.Status = "pass"
	return result
}

func (r *Runtime) setCatalog(backend host.Backend, catalog host.BackendCatalog) {
	catalog = cloneBackendCatalog(catalog)
	r.mu.Lock()
	r.catalog[backend] = catalog
	r.mu.Unlock()
}

func (r *Runtime) notifyCatalog(backend host.Backend, catalog host.BackendCatalog) {
	r.mu.Lock()
	updated := r.catalogUpdated
	r.mu.Unlock()
	if updated != nil {
		updated(backend, cloneBackendCatalog(catalog))
	}
}

func emptyCatalog(catalog host.BackendCatalog) bool {
	return len(catalog.Models) == 0 &&
		len(catalog.PermissionModes) == 0 &&
		len(catalog.Reasoning) == 0 &&
		len(catalog.SlashCommands) == 0
}

func (r *Runtime) adapterLocked(id string) (*managedSession, host.Adapter, error) {
	managed := r.sessions[id]
	if managed == nil {
		return nil, nil, &SessionNotFoundError{SessionID: id}
	}
	adapter := r.adapters[managed.session.Backend]
	if adapter == nil {
		return nil, nil, fmt.Errorf("runtime: unsupported backend %q", managed.session.Backend)
	}
	return managed, adapter, nil
}

func (r *Runtime) sessionSink(sessionID string) host.EventSink {
	return func(event host.Event) {
		r.deliver(sessionID, event)
	}
}

func (r *Runtime) deliver(sessionID string, event host.Event) {
	r.mu.Lock()
	managed := r.sessions[sessionID]
	if managed == nil {
		r.mu.Unlock()
		return
	}
	if event.SessionID == "" {
		event.SessionID = sessionID
	}
	if event.Backend == "" {
		event.Backend = managed.session.Backend
	}
	if event.BackendSessionID == "" {
		event.BackendSessionID = managed.session.BackendSession.ID
	}
	if event.BackendThreadID == "" {
		event.BackendThreadID = managed.session.BackendSession.ThreadID
	}
	if event.BackendTurnID == "" && event.Type != host.EventTurnStarted {
		if managed.activeTurnID != "" {
			event.BackendTurnID = managed.activeTurnID
		} else if !managed.active && !managed.dispatching {
			event.BackendTurnID = managed.session.BackendSession.TurnID
		}
	}
	if event.Time == "" {
		event.Time = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if event.Seq == 0 {
		managed.nextSeq++
		event.Seq = managed.nextSeq
	}
	reconcileConfiguration(managed, event)
	if event.SourceEventID == "" {
		event.SourceEventID = fmt.Sprintf("%s:%d", event.SessionID, event.Seq)
	}
	if !r.coalesceEvents {
		r.mu.Unlock()
		r.deliverNow(sessionID, event)
		return
	}
	if managed.coalescer == nil {
		managed.coalescer = newDeltaCoalescer(r.coalesceInterval, func(coalesced host.Event) {
			r.deliverNow(sessionID, coalesced)
		})
	}
	coalescer := managed.coalescer
	r.mu.Unlock()

	if event.Type == host.EventQueueUpdated || event.Type == host.EventTraceUpdated {
		r.deliverNow(sessionID, event)
		return
	}
	coalescer.Handle(event)
}

func (r *Runtime) deliverNow(sessionID string, event host.Event) {
	r.mu.Lock()
	managed := r.sessions[sessionID]
	if managed == nil {
		r.mu.Unlock()
		return
	}
	dispatchNext := false
	//exhaustive:ignore Provider-specific events are forwarded without runtime state changes.
	switch event.Type {
	case host.EventTurnStarted:
		if event.BackendTurnID != "" {
			managed.dispatching = false
			managed.active = true
			managed.activeTurnID = event.BackendTurnID
			if managed.session.Status == session.StatusActive {
				_ = managed.session.Transition(session.StatusRunning)
			}
		}
	case host.EventInteractionRequested:
		managed.pendingInteraction = cloneInteractionRequest(event.Interaction)
		if managed.session.Status == session.StatusRunning {
			_ = managed.session.Transition(session.StatusWaitingInput)
		}
	case host.EventInteractionResolved:
		if event.InteractionResponse == nil || managed.pendingInteraction == nil || event.InteractionResponse.RequestID == managed.pendingInteraction.ID {
			managed.pendingInteraction = nil
		}
	case host.EventTurnComplete, host.EventTurnFailed, host.EventProcessExited:
		terminalMatches := event.Type == host.EventProcessExited || event.BackendTurnID == "" || managed.activeTurnID == "" || event.BackendTurnID == managed.activeTurnID
		if terminalMatches {
			managed.dispatching = false
			managed.active = false
			managed.activeTurnID = ""
			managed.pendingInteraction = nil
			if managed.session.Status == session.StatusRunning || managed.session.Status == session.StatusWaitingInput {
				_ = managed.session.Transition(session.StatusActive)
			}
			dispatchNext = len(managed.queue) > 0 && !managed.dispatchBlocked
		}
	default:
	}
	emit := r.emit
	store := r.journal
	r.mu.Unlock()

	if store != nil {
		if record, ok := journal.Translate(event); ok {
			if _, err := store.Append(record); err != nil {
				event.Data = cloneData(event.Data)
				event.Data["persistence_error"] = err.Error()
			}
		}
	}
	if emit != nil {
		emit(event)
	}
	if dispatchNext {
		// Adapters commonly emit turn completion while SendTurn still holds its
		// per-session prompt lock. Dispatching inline would re-enter SendTurn
		// before that lock is released and deadlock a real ACP subprocess.
		go r.dispatchQueued(sessionID)
	}
	if event.Type != host.EventQueueUpdated {
		r.emitQueue(sessionID)
	}
}

func (r *Runtime) dispatchQueued(sessionID string) {
	r.mu.Lock()
	managed, adapter, err := r.adapterLocked(sessionID)
	if err != nil || managed.active || managed.dispatching || managed.dispatchBlocked || len(managed.queue) == 0 || managed.session.Status == session.StatusClosed {
		r.mu.Unlock()
		return
	}
	next := cloneQueueEntry(managed.queue[0])
	managed.queue = append([]QueueEntry(nil), managed.queue[1:]...)
	managed.dispatching = true
	managed.active = false
	managed.activeTurnID = ""
	r.mu.Unlock()
	r.emitQueue(sessionID)
	dispatch := TurnDispatch{SessionID: sessionID, QueueEntryID: next.ID, Request: next.Request, Queued: true}
	_, callErr := r.submitTurn(context.Background(), sessionID, adapter, dispatch)
	if callErr != nil {
		r.dispatchQueued(sessionID)
		return
	}
}

func (r *Runtime) emitQueue(sessionID string) {
	r.mu.Lock()
	managed := r.sessions[sessionID]
	if managed == nil {
		r.mu.Unlock()
		return
	}
	event := host.Event{
		SessionID: sessionID,
		Backend:   managed.session.Backend,
		Type:      host.EventQueueUpdated,
		Data: map[string]any{
			"queue_depth":      len(managed.queue),
			"active":           managed.active,
			"active_turn_id":   managed.activeTurnID,
			"dispatch_blocked": managed.dispatchBlocked,
			"max_depth":        r.maxQueuedTurns,
		},
	}
	r.mu.Unlock()
	r.deliver(sessionID, event)
}

func (r *Runtime) submitTurn(ctx context.Context, sessionID string, adapter host.Adapter, dispatch TurnDispatch) (host.BackendSession, error) {
	r.mu.Lock()
	dispatched := r.turnDispatched
	r.mu.Unlock()
	if dispatched != nil {
		if err := dispatched(dispatch); err != nil {
			r.failSubmission(sessionID, dispatch.QueueEntryID, err) //nolint:contextcheck // Failure delivery may dispatch the next queued turn.
			return host.BackendSession{}, err
		}
	}
	r.mu.Lock()
	versions := configurationEventVersions{}
	if current := r.sessions[sessionID]; current != nil {
		versions = current.configurationEvent
	}
	r.mu.Unlock()
	state, err := adapter.SendTurn(ctx, sessionID, dispatch.Request, r.sessionSink(sessionID)) //nolint:contextcheck // Adapter callbacks may dispatch queued work after the request returns.
	if err != nil {
		r.failSubmission(sessionID, dispatch.QueueEntryID, err) //nolint:contextcheck // Failure delivery may dispatch the next queued turn.
		return host.BackendSession{}, err
	}
	r.mu.Lock()
	if current := r.sessions[sessionID]; current != nil {
		current.session.BackendSession = state
		mergeUnreportedConfiguration(current, configurationFromTurn(dispatch.Request), versions)
	}
	submitted := r.turnSubmitted
	r.mu.Unlock()
	if submitted != nil {
		submitted(TurnSubmission{TurnDispatch: dispatch, Session: state})
	}
	return state, nil
}

func (r *Runtime) failSubmission(sessionID, queueEntryID string, err error) {
	r.mu.Lock()
	if current := r.sessions[sessionID]; current != nil {
		current.dispatching = false
		current.active = false
		current.activeTurnID = ""
		if current.session.Status == session.StatusRunning {
			_ = current.session.Transition(session.StatusActive)
		}
	}
	r.mu.Unlock()
	r.deliver(sessionID, host.Event{
		Type:    host.EventTurnFailed,
		Message: err.Error(),
		Data: map[string]any{
			"queue_entry_id":    queueEntryID,
			"submission_failed": true,
		},
	})
}

func (r *Runtime) interruptActive(ctx context.Context, sessionID string, adapter host.Adapter) error {
	return adapter.Interrupt(ctx, sessionID, r.sessionSink(sessionID)) //nolint:contextcheck // Adapter callbacks may dispatch queued work after the request returns.
}

func (r *Runtime) appendLifecycle(sessionID, name string, data map[string]any) error {
	if r.journal == nil {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("runtime: encode %s: %w", name, err)
	}
	_, err = r.journal.Append(journal.Record{SessionID: sessionID, Event: name, Data: raw})
	return err
}

func (r *Runtime) loadCatalogCache() {
	if r.catalogCacheDir == "" {
		return
	}
	r.mu.Lock()
	if len(r.catalog) > 0 {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	raw, err := os.ReadFile(filepath.Join(r.catalogCacheDir, "model-catalog.json"))
	if err != nil {
		return
	}
	var cache map[host.Backend]host.BackendCatalog
	if json.Unmarshal(raw, &cache) != nil {
		return
	}
	r.mu.Lock()
	if len(r.catalog) == 0 {
		r.catalog = cloneCatalog(cache)
	}
	r.mu.Unlock()
}

//nolint:govet // Cache write errors are best-effort and local to each operation.
func (r *Runtime) saveCatalogCache() {
	if r.catalogCacheDir == "" {
		return
	}
	r.catalogSaveMu.Lock()
	defer r.catalogSaveMu.Unlock()
	r.mu.Lock()
	cache := cloneCatalog(r.catalog)
	r.mu.Unlock()
	raw, err := json.Marshal(cache)
	if err != nil {
		return
	}
	if err := os.MkdirAll(r.catalogCacheDir, 0o700); err != nil {
		return
	}
	path := filepath.Join(r.catalogCacheDir, "model-catalog.json")
	temporary, err := os.CreateTemp(r.catalogCacheDir, ".model-catalog-*")
	if err != nil {
		return
	}
	defer func() { _ = os.Remove(temporary.Name()) }()
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return
	}
	if err := temporary.Close(); err != nil {
		return
	}
	_ = os.Rename(temporary.Name(), path)
}

func cloneCatalog(source map[host.Backend]host.BackendCatalog) map[host.Backend]host.BackendCatalog {
	result := make(map[host.Backend]host.BackendCatalog, len(source))
	for backend, catalog := range source {
		result[backend] = cloneBackendCatalog(catalog)
	}
	return result
}

func cloneBackendCatalog(source host.BackendCatalog) host.BackendCatalog {
	result := source
	result.Models = make([]host.BackendModel, len(source.Models))
	for index, model := range source.Models {
		result.Models[index] = model
		result.Models[index].Reasoning = append([]host.BackendReasoning(nil), model.Reasoning...)
	}
	result.PermissionModes = append([]host.BackendPermissionMode(nil), source.PermissionModes...)
	result.Reasoning = append([]host.BackendReasoning(nil), source.Reasoning...)
	result.SlashCommands = append([]host.BackendSlashCommand(nil), source.SlashCommands...)
	return result
}

func cloneData(data map[string]any) map[string]any {
	result := make(map[string]any, len(data)+1)
	maps.Copy(result, data)
	return result
}

func stateLocked(managed *managedSession) State {
	return State{
		Session:            managed.session,
		Configuration:      managed.configuration,
		QueueDepth:         len(managed.queue),
		QueueEntries:       cloneQueueEntries(managed.queue),
		TurnActive:         managed.active,
		ActiveTurnID:       managed.activeTurnID,
		DispatchBlocked:    managed.dispatchBlocked,
		PendingInteraction: cloneInteractionRequest(managed.pendingInteraction),
	}
}

func configurationFromStart(request host.StartSessionRequest) host.SessionConfiguration {
	return normalizeConfiguration(host.SessionConfiguration{
		Model: request.Model, Reasoning: request.Reasoning, PermissionMode: request.PermissionMode,
	})
}

func configurationFromTurn(request host.SendTurnRequest) host.SessionConfiguration {
	return normalizeConfiguration(host.SessionConfiguration{
		Model: request.Model, Reasoning: request.Reasoning, PermissionMode: request.PermissionMode,
	})
}

func normalizeConfiguration(configuration host.SessionConfiguration) host.SessionConfiguration {
	configuration.Model = strings.TrimSpace(configuration.Model)
	configuration.Reasoning = strings.TrimSpace(configuration.Reasoning)
	configuration.PermissionMode = strings.TrimSpace(configuration.PermissionMode)
	return configuration
}

func mergeConfiguration(current, update host.SessionConfiguration) host.SessionConfiguration {
	update = normalizeConfiguration(update)
	if update.Model != "" {
		current.Model = update.Model
	}
	if update.Reasoning != "" {
		current.Reasoning = update.Reasoning
	}
	if update.PermissionMode != "" {
		current.PermissionMode = update.PermissionMode
	}
	return normalizeConfiguration(current)
}

func fillStartConfiguration(request host.StartSessionRequest, configuration host.SessionConfiguration) host.StartSessionRequest {
	if strings.TrimSpace(request.Model) == "" {
		request.Model = configuration.Model
	}
	if strings.TrimSpace(request.Reasoning) == "" {
		request.Reasoning = configuration.Reasoning
	}
	if strings.TrimSpace(request.PermissionMode) == "" {
		request.PermissionMode = configuration.PermissionMode
	}
	return request
}

func fillTurnConfiguration(request host.SendTurnRequest, configuration host.SessionConfiguration) host.SendTurnRequest {
	if strings.TrimSpace(request.Model) == "" {
		request.Model = configuration.Model
	}
	if strings.TrimSpace(request.Reasoning) == "" {
		request.Reasoning = configuration.Reasoning
	}
	if strings.TrimSpace(request.PermissionMode) == "" {
		request.PermissionMode = configuration.PermissionMode
	}
	return request
}

func reconcileConfiguration(managed *managedSession, event host.Event) {
	if managed == nil || event.Data == nil {
		return
	}
	//exhaustive:ignore Only generic configuration events affect this state.
	switch event.Type {
	case host.EventModels:
		reconcileConfigurationValue(managed, event.Data, "current_model", "model")
	case host.EventReasoningLevels:
		reconcileConfigurationValue(managed, event.Data, "current_reasoning", "reasoning")
	case host.EventPermissionModes:
		reconcileConfigurationValue(managed, event.Data, "current_mode", "permission_mode")
	case host.EventConfigCatalog:
		reconcileConfigurationValue(managed, event.Data, "current_model", "model")
		reconcileConfigurationValue(managed, event.Data, "current_reasoning", "reasoning")
		reconcileConfigurationValue(managed, event.Data, "current_mode", "permission_mode")
	default:
		return
	}
}

func reconcileConfigurationValue(managed *managedSession, data map[string]any, key, field string) {
	raw, ok := data[key]
	if !ok {
		return
	}
	value, ok := raw.(string)
	if !ok {
		return
	}
	value = strings.TrimSpace(value)
	switch field {
	case "model":
		managed.configuration.Model = value
		managed.configurationEvent.model++
	case "reasoning":
		managed.configuration.Reasoning = value
		managed.configurationEvent.reasoning++
	case "permission_mode":
		managed.configuration.PermissionMode = value
		managed.configurationEvent.permission++
	}
}

func mergeUnreportedConfiguration(managed *managedSession, update host.SessionConfiguration, before configurationEventVersions) {
	if managed == nil {
		return
	}
	update = normalizeConfiguration(update)
	if update.Model != "" && managed.configurationEvent.model == before.model {
		managed.configuration.Model = update.Model
	}
	if update.Reasoning != "" && managed.configurationEvent.reasoning == before.reasoning {
		managed.configuration.Reasoning = update.Reasoning
	}
	if update.PermissionMode != "" && managed.configurationEvent.permission == before.permission {
		managed.configuration.PermissionMode = update.PermissionMode
	}
}

func managedConfiguration(r *Runtime, sessionID string) host.SessionConfiguration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if managed := r.sessions[sessionID]; managed != nil {
		return managed.configuration
	}
	return host.SessionConfiguration{}
}

func cloneQueueEntries(entries []QueueEntry) []QueueEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]QueueEntry, len(entries))
	for i := range entries {
		cloned[i] = cloneQueueEntry(entries[i])
	}
	return cloned
}

func cloneQueueEntry(entry QueueEntry) QueueEntry {
	entry.Request = cloneSendTurnRequest(entry.Request)
	return entry
}

func cloneSendTurnRequest(request host.SendTurnRequest) host.SendTurnRequest {
	request.Ext = append(json.RawMessage(nil), request.Ext...)
	request.Attachments = append([]host.Attachment(nil), request.Attachments...)
	return request
}

func cloneInteractionRequest(request *host.InteractionRequest) *host.InteractionRequest {
	if request == nil {
		return nil
	}
	cloned := *request
	cloned.Options = append([]host.InteractionOption(nil), request.Options...)
	cloned.Fields = make([]host.InteractionField, len(request.Fields))
	for index, field := range request.Fields {
		cloned.Fields[index] = field
		cloned.Fields[index].Options = append([]host.InteractionOption(nil), field.Options...)
	}
	cloned.Data = cloneData(request.Data)
	return &cloned
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("runtime: create session ID: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
