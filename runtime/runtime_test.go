package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/host"
	"github.com/meloniteai/durable-acp/journal"
	"github.com/meloniteai/durable-acp/session"
)

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestRuntimeQueuesAndDispatchesTurns(t *testing.T) {
	adapter := &testAdapter{backend: "test"}
	runtime := New(Config{}, adapter)
	created, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if created.Session.Status != session.StatusActive {
		t.Fatalf("created status = %q", created.Session.Status)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "first"}); err != nil {
		t.Fatal(err)
	}
	queued, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if !queued.Queued || queued.QueueDepth != 1 {
		t.Fatalf("queued result = %+v", queued)
	}
	adapter.emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: "turn-1"})
	deadline := time.Now().Add(time.Second)
	for {
		if got := adapter.prompts(); len(got) == 2 {
			if got[0] != "first" || got[1] != "second" {
				t.Fatalf("prompts = %#v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued prompt was not dispatched: %#v", adapter.prompts())
		}
		time.Sleep(time.Millisecond)
	}
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.QueueDepth != 0 || !state.TurnActive {
		t.Fatalf("state after dispatch = %+v", state)
	}
}

func TestRuntimeInteractionRoundTrip(t *testing.T) {
	adapter := &testAdapter{backend: "test"}
	runtime := New(Config{}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "question"}); err != nil {
		t.Fatal(err)
	}
	adapter.emit(host.Event{Type: host.EventInteractionRequested, Interaction: &host.InteractionRequest{ID: "interaction-1", Kind: host.InteractionForm}})
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Session.Status != session.StatusWaitingInput {
		t.Fatalf("status after interaction = %q", state.Session.Status)
	}
	if err := runtime.RespondInteraction(context.Background(), "session-1", host.InteractionResponse{RequestID: "interaction-1", Action: "submit", Values: map[string]any{"answer": "yes"}}); err != nil {
		t.Fatal(err)
	}
	if got := adapter.response(); got.RequestID != "interaction-1" || got.Values["answer"] != "yes" {
		t.Fatalf("response = %+v", got)
	}
}

func TestRuntimeInitialPromptUsesAdapterLifecycle(t *testing.T) {
	adapter := &testAdapter{backend: "test"}
	runtime := New(Config{}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1", Prompt: "initial"}); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.TurnActive || state.Session.Status != session.StatusActive {
		t.Fatalf("state after completed initial prompt = %+v", state)
	}
}

func TestRuntimeCoalescesAssistantDeltas(t *testing.T) {
	adapter := &testAdapter{backend: "test"}
	var mu sync.Mutex
	messages := []host.Event{}
	runtime := New(Config{
		CoalesceInterval: time.Hour,
		EventSink: func(event host.Event) {
			if event.Type == host.EventMessage {
				mu.Lock()
				messages = append(messages, event)
				mu.Unlock()
			}
		},
	}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	for _, delta := range []string{"a", "b", "c"} {
		adapter.emit(host.Event{Type: host.EventMessage, Role: "assistant", BackendTurnID: "turn-1", Message: delta, Data: map[string]any{"delta": delta}})
	}
	adapter.emit(host.Event{Type: host.EventToolStarted, BackendTurnID: "turn-1"})
	mu.Lock()
	defer mu.Unlock()
	if len(messages) != 2 {
		t.Fatalf("messages = %+v, want initial and coalesced snapshot", messages)
	}
	if messages[0].Message != "a" || messages[1].Message != "abc" {
		t.Fatalf("messages = %+v", messages)
	}
	if messages[0].SourceEventID != messages[1].SourceEventID {
		t.Fatalf("stream source IDs differ: %+v", messages)
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestRuntimeRestoresLifecycleFromJournal(t *testing.T) {
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	adapter := &testAdapter{backend: "test"}
	runtime := New(Config{Journal: store}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close("session-1"); err != nil {
		t.Fatal(err)
	}

	restored, err := New(Config{Journal: store}, adapter).Restore("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if restored.Session.Status != session.StatusClosed || restored.Session.Backend != "test" {
		t.Fatalf("restored = %+v", restored)
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestRuntimeValidationQueueInterruptAndClose(t *testing.T) {
	if _, err := (*Runtime)(nil).Create(context.Background(), CreateRequest{}); err == nil {
		t.Fatal("nil runtime accepted Create")
	}
	worktree := t.TempDir()
	adapter := &testAdapter{backend: "test", sendErr: errors.New("send failed")}
	runtime := New(Config{MaxQueuedTurns: 1}, adapter)
	for _, request := range []CreateRequest{
		{Worktree: worktree},
		{Backend: "test"},
		{Backend: "test", Worktree: filepath.Join(worktree, "missing")},
		{Backend: "other", Worktree: worktree},
	} {
		if _, err := runtime.Create(context.Background(), request); err == nil {
			t.Fatalf("Create accepted %#v", request)
		}
	}
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "s", Backend: "test", Worktree: worktree}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "s", Backend: "test", Worktree: worktree}); err == nil {
		t.Fatal("Create accepted duplicate ID")
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{}); err == nil {
		t.Fatal("Start accepted no session ID")
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "missing"}); err == nil {
		t.Fatal("Start accepted missing session")
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "s"}); err == nil {
		t.Fatal("Send accepted empty turn")
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "s", Prompt: "fails"}); err == nil {
		t.Fatal("Send accepted adapter failure")
	}
	state, err := runtime.State("s")
	if err != nil || state.TurnActive || state.Session.Status != session.StatusActive {
		t.Fatalf("state after failed send = %#v, %v", state, err)
	}
	adapter.sendErr = nil
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "s", Attachments: []host.Attachment{{Path: "file"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "s", Prompt: "queued"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "s", Prompt: "overflow"}); err == nil {
		t.Fatal("Send accepted a full queue")
	}
	if err := runtime.Interrupt(context.Background(), "s"); err != nil {
		t.Fatal(err)
	}
	state, err = runtime.State("s")
	if err != nil || state.QueueDepth != 0 || adapter.interrupts != 1 {
		t.Fatalf("state after interrupt = %#v, adapter = %#v", state, adapter)
	}
	adapter.closeErr = errors.New("close failed")
	if err := runtime.Close("s"); err == nil {
		t.Fatal("Close accepted adapter failure")
	}
	adapter.closeErr = nil
	if err := runtime.Close("s"); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "s", Prompt: "closed"}); err == nil {
		t.Fatal("Send accepted closed session")
	}
	if err := runtime.Close("s"); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeCatalogForkDetectAndCache(t *testing.T) {
	directory := t.TempDir()
	adapter := &testAdapter{backend: "b", catalog: host.BackendCatalog{Models: []host.BackendModel{{ID: "model"}}}}
	second := &testAdapter{backend: "a"}
	cacheReady := make(chan error, 1)
	runtime := New(Config{
		CatalogCacheDir: directory,
		CatalogUpdated: func(_ host.Backend, _ host.BackendCatalog) {
			_, err := os.Stat(filepath.Join(directory, "model-catalog.json"))
			cacheReady <- err
		},
	}, adapter, second, nil)
	statuses := runtime.Detect(context.Background())
	if len(statuses) != 2 || statuses[0].Backend != "a" || statuses[1].Backend != "b" {
		t.Fatalf("statuses = %#v", statuses)
	}
	catalog := runtime.Catalog(context.Background(), true)
	if len(catalog["b"].Models) != 1 {
		t.Fatalf("catalog = %#v", catalog)
	}
	if err := <-cacheReady; err != nil {
		t.Fatalf("catalog callback ran before its cache was durable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "model-catalog.json")); err != nil {
		t.Fatalf("catalog cache missing: %v", err)
	}
	catalog["b"] = host.BackendCatalog{}
	if got := runtime.Catalog(context.Background(), false); len(got["b"].Models) != 1 {
		t.Fatalf("catalog copy = %#v", got)
	}
	cached := New(Config{CatalogCacheDir: directory}, adapter).Catalog(context.Background(), false)
	if len(cached["b"].Models) != 1 {
		t.Fatalf("cached catalog = %#v", cached)
	}
	worktree := t.TempDir()
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "s", Backend: "b", Worktree: worktree}); err != nil {
		t.Fatal(err)
	}
	response, err := runtime.ForkPrompt(context.Background(), host.ForkPromptRequest{SessionID: "s", Prompt: "fork", Instructions: "keep tests"})
	if err != nil || !response.Accepted {
		t.Fatalf("ForkPrompt = %#v, %v", response, err)
	}
	if got := runtime.Sessions(); len(got) != 1 || got[0].Session.ID != "s" {
		t.Fatalf("Sessions = %#v", got)
	}
	if err := runtime.Shutdown(); err != nil || adapter.closes != 1 || second.closes != 0 {
		t.Fatalf("Shutdown = %v, closes = %d/%d", err, adapter.closes, second.closes)
	}
}

func TestRuntimeRefreshCatalogReportsAndUpdatesIncrementally(t *testing.T) {
	updates := make(chan host.BackendCatalog, 2)
	available := &catalogProbeAdapter{
		testAdapter: testAdapter{backend: "available"},
		catalog: func(context.Context) (host.BackendCatalog, error) {
			return host.BackendCatalog{Models: []host.BackendModel{{
				ID:        "model",
				Reasoning: []host.BackendReasoning{{ID: "low"}},
			}}}, nil
		},
	}
	unavailable := &catalogProbeAdapter{
		testAdapter: testAdapter{backend: "unavailable"},
		detect: func(context.Context) host.BackendStatus {
			return host.BackendStatus{Backend: "unavailable", Error: "not installed"}
		},
	}
	failing := &catalogProbeAdapter{
		testAdapter: testAdapter{backend: "failing"},
		catalog: func(context.Context) (host.BackendCatalog, error) {
			return host.BackendCatalog{}, errors.New("probe failed")
		},
	}
	timedOut := &catalogProbeAdapter{
		testAdapter: testAdapter{backend: "timed-out"},
		catalog: func(ctx context.Context) (host.BackendCatalog, error) {
			<-ctx.Done()
			return host.BackendCatalog{}, ctx.Err()
		},
	}
	runtime := New(Config{
		CatalogTimeout: 10 * time.Millisecond,
		CatalogUpdated: func(_ host.Backend, catalog host.BackendCatalog) {
			updates <- catalog
		},
	}, available, unavailable, failing, timedOut, &plainAdapter{backend: "unsupported"})

	results := runtime.RefreshCatalog(context.Background())
	if got := fmt.Sprintf("%s:%s,%s:%s,%s:%s,%s:%s,%s:%s",
		results[0].Backend, results[0].Status,
		results[1].Backend, results[1].Status,
		results[2].Backend, results[2].Status,
		results[3].Backend, results[3].Status,
		results[4].Backend, results[4].Status,
	); got != "available:pass,failing:fail,timed-out:fail,unavailable:skip,unsupported:skip" {
		t.Fatalf("results = %#v", results)
	}
	select {
	case update := <-updates:
		if update.Models[0].ID != "model" {
			t.Fatalf("update = %#v", update)
		}
	default:
		t.Fatal("successful refresh did not notify")
	}

	input := host.BackendCatalog{SlashCommands: []host.BackendSlashCommand{{Name: "review"}}}
	runtime.SetCatalog("available", input)
	input.SlashCommands[0].Name = "mutated"
	catalog := runtime.Catalog(context.Background(), false)
	if got := catalog["available"].SlashCommands[0].Name; got != "review" {
		t.Fatalf("catalog mutation escaped SetCatalog: %q", got)
	}
	catalog["available"].SlashCommands[0].Name = "changed"
	if got := runtime.Catalog(context.Background(), false)["available"].SlashCommands[0].Name; got != "review" {
		t.Fatalf("catalog mutation escaped Catalog: %q", got)
	}
}

func TestRuntimeInteractionFallbackAndRestoreFailures(t *testing.T) {
	permission := &permissionAdapter{plainAdapter: &plainAdapter{backend: "permission"}}
	runtime := New(Config{}, permission)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "s", Backend: "permission", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	permission.emit(host.Event{Type: host.EventTurnStarted})
	permission.emit(host.Event{Type: host.EventInteractionRequested, Interaction: &host.InteractionRequest{ID: "ask", Kind: host.InteractionPermission}})
	if err := runtime.RespondInteraction(context.Background(), "s", host.InteractionResponse{RequestID: "ask", Action: "allow", Message: "yes"}); err != nil {
		t.Fatal(err)
	}
	if !permission.allowed || permission.requestID != "ask" {
		t.Fatalf("permission response = %#v", permission)
	}
	if err := runtime.RespondInteraction(context.Background(), "s", host.InteractionResponse{}); err == nil {
		t.Fatal("RespondInteraction accepted empty request ID")
	}
	plain := &plainAdapter{backend: "plain"}
	unsupported := New(Config{}, plain)
	if _, err := unsupported.Create(context.Background(), CreateRequest{ID: "plain", Backend: "plain", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := unsupported.ForkPrompt(context.Background(), host.ForkPromptRequest{SessionID: "plain"}); err == nil {
		t.Fatal("ForkPrompt succeeded without adapter capability")
	}
	if err := unsupported.RespondInteraction(context.Background(), "plain", host.InteractionResponse{RequestID: "ask"}); err == nil {
		t.Fatal("interaction succeeded without adapter capability")
	}
	if _, err := (*Runtime)(nil).Restore("s"); err == nil {
		t.Fatal("nil runtime restored a session")
	}
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Journal: store}, plain).Restore("missing"); err == nil {
		t.Fatal("Restore accepted a missing lifecycle")
	}
}

func TestRuntimeStartStateAndQueuedFailurePaths(t *testing.T) {
	adapter := &testAdapter{backend: "test", startErr: errors.New("start failed")}
	runtime := New(Config{}, adapter, adapter, nil)
	if got := runtime.Sessions(); len(got) != 0 {
		t.Fatalf("new sessions = %#v", got)
	}
	if _, err := runtime.State("missing"); err == nil {
		t.Fatal("State accepted a missing session")
	}
	if err := (*Runtime)(nil).Interrupt(context.Background(), "s"); err == nil {
		t.Fatal("nil runtime interrupted a session")
	}
	if err := (*Runtime)(nil).Close("s"); err == nil {
		t.Fatal("nil runtime closed a session")
	}
	if (*Runtime)(nil).Sessions() != nil || (*Runtime)(nil).Detect(context.Background()) != nil || (*Runtime)(nil).Catalog(context.Background(), false) != nil {
		t.Fatal("nil runtime returned state")
	}
	if err := (*Runtime)(nil).Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Create(context.Background(), CreateRequest{Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	states := runtime.Sessions()
	if len(states) != 1 || states[0].Session.ID == "" {
		t.Fatalf("generated session = %#v", states)
	}
	id := states[0].Session.ID
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: id}); err == nil {
		t.Fatal("Start accepted adapter failure")
	}
	adapter.startErr = nil
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: id, Prompt: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: id, Prompt: "queued"}); err != nil {
		t.Fatal(err)
	}
	adapter.sendErr = errors.New("queued failure")
	adapter.emit(host.Event{Type: host.EventTurnComplete})
	deadline := time.Now().Add(time.Second)
	for {
		state, err := runtime.State(id)
		if err != nil {
			t.Fatal(err)
		}
		if state.QueueDepth == 0 && !state.TurnActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state after queued failure = %#v", state)
		}
		time.Sleep(time.Millisecond)
	}
	if got := cloneData(map[string]any{"value": "original"}); got["value"] != "original" {
		t.Fatalf("cloneData = %#v", got)
	}
	if got := cloneData(nil); len(got) != 0 {
		t.Fatalf("nil cloneData = %#v", got)
	}
}

type testAdapter struct {
	backend      host.Backend
	mu           sync.Mutex
	sink         host.EventSink
	turns        []string
	answer       host.InteractionResponse
	startErr     error
	sendErr      error
	interruptErr error
	closeErr     error
	interrupts   int
	closes       int
	catalog      host.BackendCatalog
	catalogErr   error
}

type catalogProbeAdapter struct {
	testAdapter
	detect  func(context.Context) host.BackendStatus
	catalog func(context.Context) (host.BackendCatalog, error)
}

func (a *catalogProbeAdapter) Detect(ctx context.Context) host.BackendStatus {
	if a.detect != nil {
		return a.detect(ctx)
	}
	return a.testAdapter.Detect(ctx)
}

func (a *catalogProbeAdapter) Catalog(ctx context.Context) (host.BackendCatalog, error) {
	if a.catalog != nil {
		return a.catalog(ctx)
	}
	return a.testAdapter.Catalog(ctx)
}

func (a *testAdapter) Backend() host.Backend { return a.backend }

func (a *testAdapter) Detect(context.Context) host.BackendStatus {
	return host.BackendStatus{Backend: a.backend, Available: true}
}

func (a *testAdapter) StartSession(_ context.Context, _ string, request host.StartSessionRequest, emit host.EventSink) (host.BackendSession, error) {
	a.mu.Lock()
	a.sink = emit
	a.mu.Unlock()
	if a.startErr != nil {
		return host.BackendSession{}, a.startErr
	}
	if request.Prompt != "" {
		emit(host.Event{Type: host.EventTurnStarted, BackendTurnID: "initial-turn"})
		emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: "initial-turn"})
	}
	return host.BackendSession{ID: "provider-session"}, nil
}

func (a *testAdapter) SendTurn(_ context.Context, _ string, request host.SendTurnRequest, emit host.EventSink) (host.BackendSession, error) {
	a.mu.Lock()
	a.sink = emit
	a.turns = append(a.turns, request.Prompt)
	turnID := fmt.Sprintf("turn-%d", len(a.turns))
	a.mu.Unlock()
	if a.sendErr != nil {
		return host.BackendSession{}, a.sendErr
	}
	emit(host.Event{Type: host.EventTurnStarted, BackendTurnID: turnID})
	return host.BackendSession{ID: "provider-session", TurnID: turnID}, nil
}

func (a *testAdapter) Interrupt(context.Context, string, host.EventSink) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.interrupts++
	return a.interruptErr
}

func (a *testAdapter) CloseSession(string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closes++
	return a.closeErr
}

func (a *testAdapter) Catalog(context.Context) (host.BackendCatalog, error) {
	return a.catalog, a.catalogErr
}

func (a *testAdapter) ForkPrompt(_ context.Context, request host.ForkPromptRequest) (host.ForkPromptResponse, error) {
	return host.ForkPromptResponse{Accepted: request.SessionID != ""}, nil
}

func (a *testAdapter) RespondInteraction(_ context.Context, _ string, response host.InteractionResponse) error {
	a.mu.Lock()
	a.answer = response
	a.mu.Unlock()
	return nil
}

func (a *testAdapter) emit(event host.Event) {
	a.mu.Lock()
	sink := a.sink
	a.mu.Unlock()
	if sink != nil {
		sink(event)
	}
}

func (a *testAdapter) prompts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.turns...)
}

func (a *testAdapter) response() host.InteractionResponse {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.answer
}

type plainAdapter struct{ backend host.Backend }

func (a *plainAdapter) Backend() host.Backend { return a.backend }

func (a *plainAdapter) Detect(context.Context) host.BackendStatus {
	return host.BackendStatus{Backend: a.backend, Available: true}
}

func (a *plainAdapter) StartSession(_ context.Context, _ string, _ host.StartSessionRequest, _ host.EventSink) (host.BackendSession, error) {
	return host.BackendSession{ID: "provider"}, nil
}

func (a *plainAdapter) SendTurn(_ context.Context, _ string, _ host.SendTurnRequest, _ host.EventSink) (host.BackendSession, error) {
	return host.BackendSession{ID: "provider", TurnID: "turn"}, nil
}

func (a *plainAdapter) Interrupt(context.Context, string, host.EventSink) error { return nil }

func (a *plainAdapter) CloseSession(string) error { return nil }

type permissionAdapter struct {
	*plainAdapter
	sink      host.EventSink
	allowed   bool
	requestID string
}

func (a *permissionAdapter) StartSession(ctx context.Context, id string, request host.StartSessionRequest, sink host.EventSink) (host.BackendSession, error) {
	a.sink = sink
	return a.plainAdapter.StartSession(ctx, id, request, sink)
}

func (a *permissionAdapter) SendTurn(ctx context.Context, id string, request host.SendTurnRequest, sink host.EventSink) (host.BackendSession, error) {
	a.sink = sink
	return a.plainAdapter.SendTurn(ctx, id, request, sink)
}

func (a *permissionAdapter) RespondPermission(
	_ context.Context,
	_ string,
	requestID string,
	allow bool,
	_ string,
	_ string,
) error {
	a.requestID = requestID
	a.allowed = allow
	return nil
}

func (a *permissionAdapter) emit(event host.Event) {
	if a.sink != nil {
		a.sink(event)
	}
}
