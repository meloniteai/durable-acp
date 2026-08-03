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

func TestRuntimeQueueEntriesRemovalBlockingAndDispatchLifecycle(t *testing.T) {
	adapter := &testAdapter{backend: "test"}
	var mu sync.Mutex
	var dispatched []TurnDispatch
	runtime := New(Config{
		MaxQueuedTurns:    4,
		DisableCoalescing: true,
		TurnDispatched: func(dispatch TurnDispatch) error {
			mu.Lock()
			dispatched = append(dispatched, dispatch)
			mu.Unlock()
			return nil
		},
	}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "active"}); err != nil {
		t.Fatal(err)
	}
	first, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if first.QueueEntryID == "" || second.QueueEntryID == "" || first.QueueEntryID == second.QueueEntryID {
		t.Fatalf("queue IDs = %q, %q", first.QueueEntryID, second.QueueEntryID)
	}
	entries, err := runtime.QueueEntries("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].ID != first.QueueEntryID || entries[1].ID != second.QueueEntryID {
		t.Fatalf("entries = %+v", entries)
	}
	entries[0].Request.Prompt = "mutated"
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveTurnID != "turn-1" || len(state.QueueEntries) != 2 || state.QueueEntries[0].Request.Prompt != "first" {
		t.Fatalf("state = %+v", state)
	}
	removed, err := runtime.RemoveQueuedTurn("session-1", first.QueueEntryID)
	if err != nil {
		t.Fatal(err)
	}
	if !removed.Removed || removed.Entry.ID != first.QueueEntryID || removed.QueueDepth != 1 {
		t.Fatalf("removed = %+v", removed)
	}
	missing, err := runtime.RemoveQueuedTurn("session-1", "missing")
	if err != nil || missing.Removed || missing.QueueDepth != 1 {
		t.Fatalf("missing removal = %+v, %v", missing, err)
	}
	if blockErr := runtime.BlockDispatch("session-1"); blockErr != nil {
		t.Fatal(blockErr)
	}
	adapter.emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: "turn-1"})
	time.Sleep(20 * time.Millisecond)
	if got := adapter.prompts(); len(got) != 1 {
		t.Fatalf("prompts while blocked = %#v", got)
	}
	priority, err := runtime.SendNext(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "priority"})
	if err != nil {
		t.Fatal(err)
	}
	if _, replaceErr := runtime.ReplaceQueuedTurn("session-1", priority.QueueEntryID, host.SendTurnRequest{Prompt: "replacement"}); replaceErr != nil {
		t.Fatal(replaceErr)
	}
	state, err = runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if !state.DispatchBlocked || len(state.QueueEntries) != 2 || state.QueueEntries[0].ID != priority.QueueEntryID || state.QueueEntries[0].Request.Prompt != "replacement" || state.QueueEntries[1].ID != second.QueueEntryID {
		t.Fatalf("blocked state = %+v", state)
	}
	if err := runtime.UnblockDispatch("session-1"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(adapter.prompts()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := adapter.prompts(); len(got) != 2 || got[1] != "replacement" {
		t.Fatalf("prompts after unblock = %#v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dispatched) != 2 || dispatched[0].Queued || !dispatched[1].Queued || dispatched[1].QueueEntryID != priority.QueueEntryID {
		t.Fatalf("dispatches = %+v", dispatched)
	}
}

func TestRuntimeRestartKeepsLogicalSessionLive(t *testing.T) {
	adapter := &restartTestAdapter{testAdapter: testAdapter{backend: "test"}}
	runtime := New(Config{DisableCoalescing: true}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "active"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Restart(context.Background(), "session-1"); err == nil {
		t.Fatal("Restart accepted an active turn")
	} else {
		var active *TurnActiveError
		if !errors.As(err, &active) || active.TurnID != "turn-1" {
			t.Fatalf("active restart error = %T %v", err, err)
		}
	}
	adapter.emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: "turn-1"})
	if err := runtime.BlockDispatch("session-1"); err != nil {
		t.Fatal(err)
	}
	queued, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "after restart"})
	if err != nil || !queued.Queued {
		t.Fatalf("queued before restart = %+v, %v", queued, err)
	}
	restarted, err := runtime.Restart(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if restarted.ID != "provider-restarted" || adapter.restarts != 1 {
		t.Fatalf("restart = %+v, count = %d", restarted, adapter.restarts)
	}
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Session.Status == session.StatusClosed || state.Session.BackendSession.ID != "provider-restarted" || state.TurnActive || state.ActiveTurnID != "" || state.QueueDepth != 1 || !state.DispatchBlocked {
		t.Fatalf("state after restart = %+v", state)
	}
	plain := New(Config{}, &plainAdapter{backend: "plain"})
	if _, err := plain.Create(context.Background(), CreateRequest{ID: "plain", Backend: "plain", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Restart(context.Background(), "plain"); err == nil {
		t.Fatal("Restart accepted unsupported adapter")
	} else {
		var unsupported *RestartUnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("unsupported restart error = %T %v", err, err)
		}
	}
}

func TestRuntimeStartRequiresTurnIdentity(t *testing.T) {
	runtime := New(Config{}, &plainAdapter{backend: "plain"})
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "plain", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "plain", Worktree: t.TempDir()}); err == nil {
		t.Fatal("Create accepted a duplicate session")
	} else {
		var exists *SessionExistsError
		if !errors.As(err, &exists) || exists.SessionID != "session-1" {
			t.Fatalf("duplicate error = %T %v", err, err)
		}
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.TurnActive || state.ActiveTurnID != "" {
		t.Fatalf("prompt start state = %#v", state)
	}
	anonymous := New(Config{}, &lifecycleStartAdapter{plainAdapter: plainAdapter{backend: "anonymous"}})
	if _, createErr := anonymous.Create(context.Background(), CreateRequest{ID: "anonymous", Backend: "anonymous", Worktree: t.TempDir()}); createErr != nil {
		t.Fatal(createErr)
	}
	if _, startErr := anonymous.Start(context.Background(), host.StartSessionRequest{SessionID: "anonymous"}); startErr != nil {
		t.Fatal(startErr)
	}
	anonymousState, err := anonymous.State("anonymous")
	if err != nil {
		t.Fatal(err)
	}
	if anonymousState.TurnActive || anonymousState.ActiveTurnID != "" || anonymousState.Session.Status != session.StatusActive {
		t.Fatalf("anonymous start state = %#v", anonymousState)
	}
	identified := New(Config{}, &lifecycleStartAdapter{plainAdapter: plainAdapter{backend: "identified"}, turnID: "initial-turn"})
	if _, createErr := identified.Create(context.Background(), CreateRequest{ID: "identified", Backend: "identified", Worktree: t.TempDir()}); createErr != nil {
		t.Fatal(createErr)
	}
	if _, startErr := identified.Start(context.Background(), host.StartSessionRequest{SessionID: "identified"}); startErr != nil {
		t.Fatal(startErr)
	}
	identifiedState, err := identified.State("identified")
	if err != nil {
		t.Fatal(err)
	}
	if !identifiedState.TurnActive || identifiedState.ActiveTurnID != "initial-turn" || identifiedState.Session.Status != session.StatusRunning {
		t.Fatalf("identified start state = %#v", identifiedState)
	}
}

func TestRuntimeReservesDispatchBeforeIdentifiedStart(t *testing.T) {
	adapter := &testAdapter{backend: "test"}
	entered := make(chan struct{})
	release := make(chan struct{})
	runtime := New(Config{TurnDispatched: func(TurnDispatch) error {
		close(entered)
		<-release
		return nil
	}}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "first"})
		firstDone <- err
	}()
	<-entered
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.TurnActive || state.ActiveTurnID != "" {
		t.Fatalf("pre-start state = %#v", state)
	}
	queued, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "second"})
	if err != nil || !queued.Queued || queued.QueueDepth != 1 {
		t.Fatalf("reserved dispatch queue result = %#v, %v", queued, err)
	}
	close(release)
	if sendErr := <-firstDone; sendErr != nil {
		t.Fatal(sendErr)
	}
	state, err = runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if !state.TurnActive || state.ActiveTurnID != "turn-1" || state.QueueDepth != 1 {
		t.Fatalf("identified state = %#v", state)
	}
}

func TestRuntimeDoesNotReuseLastTurnIDForAnonymousStart(t *testing.T) {
	adapter := &testAdapter{backend: "strict"}
	var events []host.Event
	runtime := New(Config{DisableCoalescing: true, EventSink: func(event host.Event) {
		events = append(events, event)
	}}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "strict", Backend: "strict", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "strict"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "strict", Prompt: "first"}); err != nil {
		t.Fatal(err)
	}
	adapter.emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: "turn-1"})
	adapter.emit(host.Event{Type: host.EventTurnStarted})
	state, err := runtime.State("strict")
	if err != nil {
		t.Fatal(err)
	}
	if state.TurnActive || state.ActiveTurnID != "" {
		t.Fatalf("anonymous start reused prior turn identity: %#v", state)
	}
	if event := events[len(events)-2]; event.Type != host.EventTurnStarted || event.BackendTurnID != "" {
		t.Fatalf("anonymous start = %#v", event)
	}
}

func TestRuntimeUsesActiveTurnIDForAdapterStream(t *testing.T) {
	adapter := &identityStreamAdapter{backend: "stream"}
	var messages []host.Event
	runtime := New(Config{DisableCoalescing: true, EventSink: func(event host.Event) {
		if event.Type == host.EventMessage {
			messages = append(messages, event)
		}
	}}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "stream", Backend: "stream", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "stream"}); err != nil {
		t.Fatal(err)
	}
	for _, prompt := range []string{"first", "second"} {
		if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "stream", Prompt: prompt}); err != nil {
			t.Fatal(err)
		}
	}
	if len(messages) != 2 || messages[0].BackendTurnID != "turn-1" || messages[1].BackendTurnID != "turn-2" {
		t.Fatalf("stream messages = %#v", messages)
	}
}

func TestRuntimeInterruptActiveAndReconcileTurn(t *testing.T) {
	adapter := &testAdapter{backend: "test"}
	runtime := New(Config{}, adapter)
	if _, err := runtime.Create(context.Background(), CreateRequest{ID: "session-1", Backend: "test", Worktree: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(context.Background(), host.StartSessionRequest{SessionID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "active"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Send(context.Background(), host.SendTurnRequest{SessionID: "session-1", Prompt: "queued"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.BlockDispatch("session-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.InterruptActive(context.Background(), "session-1"); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if adapter.interrupts != 1 || !state.TurnActive || state.QueueDepth != 1 {
		t.Fatalf("state after interrupt = %#v, calls = %d", state, adapter.interrupts)
	}
	if reconcileErr := runtime.ReconcileTurn("session-1", host.Event{Type: host.EventMessage}); reconcileErr == nil {
		t.Fatal("ReconcileTurn accepted a non-terminal event")
	}
	if reconcileErr := runtime.ReconcileTurn("missing", host.Event{Type: host.EventTurnFailed}); reconcileErr == nil {
		t.Fatal("ReconcileTurn accepted a missing session")
	}
	if reconcileErr := runtime.ReconcileTurn("session-1", host.Event{Type: host.EventTurnFailed, BackendTurnID: "turn-1"}); reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	state, err = runtime.State("session-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.TurnActive || state.QueueDepth != 1 || !state.DispatchBlocked {
		t.Fatalf("reconciled state = %#v", state)
	}
	if err := runtime.UnblockDispatch("session-1"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(adapter.prompts()) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := adapter.prompts(); len(got) != 2 || got[1] != "queued" {
		t.Fatalf("prompts = %#v", got)
	}
	if err := runtime.ReconcileTurn("session-1", host.Event{Type: host.EventTurnComplete, BackendTurnID: "turn-2"}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.InterruptActive(context.Background(), "session-1"); err != nil {
		t.Fatal(err)
	}
	if err := (*Runtime)(nil).InterruptActive(context.Background(), "session-1"); err == nil {
		t.Fatal("nil runtime interrupted an active turn")
	}
	if err := (*Runtime)(nil).ReconcileTurn("session-1", host.Event{Type: host.EventTurnComplete}); err == nil {
		t.Fatal("nil runtime reconciled a turn")
	}
}

func TestRuntimeTypedErrorMessages(t *testing.T) {
	errors := []error{
		&SessionNotFoundError{SessionID: "session"},
		&SessionExistsError{SessionID: "session"},
		&SessionClosedError{SessionID: "session"},
		&QueueFullError{SessionID: "session", Depth: 3, Limit: 3},
		&QueueEntryNotFoundError{SessionID: "session", EntryID: "entry"},
		&TurnActiveError{SessionID: "session"},
		&TurnActiveError{SessionID: "session", TurnID: "turn"},
		&RestartUnsupportedError{Backend: "test"},
	}
	for _, err := range errors {
		if err.Error() == "" {
			t.Fatalf("empty error for %T", err)
		}
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
	if err := runtime.Forget("s"); err == nil {
		t.Fatal("Forget accepted active session")
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
	if err := runtime.Forget("s"); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.State("s"); err == nil {
		t.Fatal("Forget retained closed session")
	}
	if err := runtime.Forget("s"); err != nil {
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
	permission.emit(host.Event{Type: host.EventTurnStarted, BackendTurnID: "turn-1"})
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
	if err := (*Runtime)(nil).Forget("s"); err == nil {
		t.Fatal("nil runtime forgot a session")
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

type restartTestAdapter struct {
	testAdapter
	restarts int
}

func (a *restartTestAdapter) RestartSession(_ context.Context, _ string, emit host.EventSink) (host.BackendSession, error) {
	a.restarts++
	emit(host.Event{Type: host.EventAgentRecovered, BackendSessionID: "provider-restarted"})
	return host.BackendSession{ID: "provider-restarted", ThreadID: "provider-restarted"}, nil
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

type identityStreamAdapter struct {
	backend host.Backend
	turns   int
}

func (a *identityStreamAdapter) Backend() host.Backend { return a.backend }

func (a *identityStreamAdapter) Detect(context.Context) host.BackendStatus {
	return host.BackendStatus{Backend: a.backend, Available: true}
}

func (a *identityStreamAdapter) StartSession(context.Context, string, host.StartSessionRequest, host.EventSink) (host.BackendSession, error) {
	return host.BackendSession{ID: "provider"}, nil
}

func (a *identityStreamAdapter) SendTurn(_ context.Context, _ string, _ host.SendTurnRequest, emit host.EventSink) (host.BackendSession, error) {
	a.turns++
	turnID := fmt.Sprintf("turn-%d", a.turns)
	emit(host.Event{Type: host.EventTurnStarted, BackendTurnID: turnID})
	emit(host.Event{Type: host.EventMessage, Role: "assistant", Message: turnID})
	emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: turnID})
	return host.BackendSession{ID: "provider", TurnID: turnID}, nil
}

func (a *identityStreamAdapter) Interrupt(context.Context, string, host.EventSink) error {
	return nil
}

func (a *identityStreamAdapter) CloseSession(string) error { return nil }

type lifecycleStartAdapter struct {
	plainAdapter
	turnID string
}

func (a *lifecycleStartAdapter) StartSession(ctx context.Context, id string, request host.StartSessionRequest, emit host.EventSink) (host.BackendSession, error) {
	emit(host.Event{Type: host.EventTurnStarted, BackendTurnID: a.turnID})
	return a.plainAdapter.StartSession(ctx, id, request, emit)
}

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
