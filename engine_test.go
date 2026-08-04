package durableacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/host"
	"github.com/meloniteai/durable-acp/journal"
	"github.com/meloniteai/durable-acp/runtime"
)

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestEngineManagedSessionLifecycle(t *testing.T) {
	home := t.TempDir()
	engine, err := Open(context.Background(), home, WithAdapters(engineAdapter{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	for _, name := range []string{configFileName, "sessions", "journals", "logs", "worktrees", "cache"} {
		if _, err := os.Stat(filepath.Join(home, name)); err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
	}
	for _, backend := range []string{"claude", "codex", "cursor", "antigravity"} {
		if _, err := os.Stat(filepath.Join(home, "logs", "providers", backend+".log")); err != nil {
			t.Fatalf("provider log %s missing: %v", backend, err)
		}
	}

	created, err := engine.Start(context.Background(), StartRequest{Backend: "test", Source: engineRepo(t)})
	if err != nil {
		t.Fatal(err)
	}
	if created.WorkspaceMode != WorkspaceManaged || created.Worktree.Branch != "durable-acp/"+created.ID {
		t.Fatalf("created = %+v", created)
	}
	if _, err := os.Stat(created.Worktree.Path); err != nil {
		t.Fatalf("managed worktree missing: %v", err)
	}
	if _, err := engine.Append(created.ID, "example.annotation", map[string]any{"value": true}, nil); err != nil {
		t.Fatal(err)
	}
	records, err := engine.Journal().Read(created.ID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) < 3 || records[len(records)-1].Event != "example.annotation" {
		t.Fatalf("records = %+v", records)
	}
	if err := engine.CloseSession(created.ID); err != nil {
		t.Fatal(err)
	}
	if err := engine.Remove(context.Background(), created.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(created.Worktree.Path); !os.IsNotExist(err) {
		t.Fatalf("removed worktree stat error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, "sessions", created.ID+".json")); !os.IsNotExist(err) {
		t.Fatalf("removed manifest stat error = %v", err)
	}
}

func TestOpenRejectsRelativeHome(t *testing.T) {
	if _, err := Open(context.Background(), "relative"); err == nil {
		t.Fatal("Open succeeded with relative home")
	}
}

func TestOpenJournalOptions(t *testing.T) {
	storeHome := t.TempDir()
	store, err := journal.NewStore(filepath.Join(storeHome, "history"))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := Open(context.Background(), storeHome, WithJournalStore(store))
	if err != nil {
		t.Fatal(err)
	}
	if engine.Journal() != store {
		t.Fatal("Engine did not use supplied journal store")
	}
	if closeErr := engine.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if _, appendErr := store.Append(journal.Record{SessionID: "supplied", Event: "host.event"}); appendErr != nil {
		t.Fatal(appendErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	configuredHome := t.TempDir()
	configuredDirectory := filepath.Join(t.TempDir(), "events")
	configured, err := Open(context.Background(), configuredHome, WithJournalConfiguration(JournalConfiguration{
		Directory: configuredDirectory,
		Options:   []journal.Option{journal.WithSchemaID("host.events.v1")},
	}))
	if err != nil {
		t.Fatal(err)
	}
	record, err := configured.Append("configured", "host.event", nil, nil)
	if err != nil || record.Schema != "host.events.v1" {
		t.Fatalf("configured journal record = %#v, %v", record, err)
	}
	if _, err := os.Stat(filepath.Join(configuredHome, "journals")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("configured Engine created default journal directory: %v", err)
	}
	if err := configured.Close(); err != nil {
		t.Fatal(err)
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestEngineRepairRestoresManagedBranch(t *testing.T) {
	engine, err := Open(context.Background(), t.TempDir(), WithAdapters(engineAdapter{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	created, err := engine.Start(context.Background(), StartRequest{Backend: "test", Source: engineRepo(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engineGit(created.Worktree.Path, "checkout", "-b", "temporary-branch"); err != nil {
		t.Fatal(err)
	}
	repaired, err := engine.Repair(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Worktree.Branch != created.Worktree.Branch {
		t.Fatalf("repair branch = %q, want %q", repaired.Worktree.Branch, created.Worktree.Branch)
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestEngineCloseRetainsResumableSession(t *testing.T) {
	home := t.TempDir()
	adapter := &engineAdapter{}
	engine, err := Open(context.Background(), home, WithAdapters(adapter))
	if err != nil {
		t.Fatal(err)
	}
	created, err := engine.Start(context.Background(), StartRequest{Backend: "test", Source: engineRepo(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), home, WithAdapters(adapter))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Resume(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	entry, err := reopened.loadSession(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if entry.ClosedAt != nil {
		t.Fatalf("engine Close marked session closed: %+v", entry)
	}
}

func TestEngineExistingWorkspaceFacadesAndEvents(t *testing.T) {
	events := make(chan host.Event, 32)
	adapter := &advancedEngineAdapter{}
	engine, err := Open(context.Background(), t.TempDir(), WithAdapters(adapter), WithEventSink(func(event host.Event) { events <- event }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	source := engineRepo(t)
	created, err := engine.Start(context.Background(), StartRequest{ID: "existing", Backend: "advanced", WorkspaceMode: WorkspaceExisting, Worktree: source, Prompt: "start"})
	if err != nil {
		t.Fatal(err)
	}
	if created.WorkspaceMode != WorkspaceExisting || created.Worktree.Path == "" || created.Worktree.ID != "existing" {
		t.Fatalf("created = %#v", created)
	}
	if _, err := engine.Send(context.Background(), host.SendTurnRequest{SessionID: created.ID, Prompt: "turn"}); err != nil {
		t.Fatal(err)
	}
	adapter.emit(host.Event{Type: host.EventInteractionRequested, BackendTurnID: "turn", Interaction: &host.InteractionRequest{ID: "choice", Kind: host.InteractionChoice}})
	if err := engine.RespondInteraction(context.Background(), created.ID, host.InteractionResponse{RequestID: "choice", Action: "submit"}); err != nil {
		t.Fatal(err)
	}
	if adapter.answer.RequestID != "choice" {
		t.Fatalf("interaction answer = %#v", adapter.answer)
	}
	if response, err := engine.ForkPrompt(context.Background(), host.ForkPromptRequest{SessionID: created.ID, Prompt: "fork", Instructions: "continue"}); err != nil || !response.Accepted {
		t.Fatalf("fork = %#v, %v", response, err)
	}
	if err := engine.Interrupt(context.Background(), created.ID); err != nil || adapter.interrupts != 1 {
		t.Fatalf("interrupt = %v, calls = %d", err, adapter.interrupts)
	}
	resolved := 0
	for len(events) > 0 {
		if (<-events).Type == host.EventInteractionResolved {
			resolved++
		}
	}
	if resolved != 1 {
		t.Fatalf("interaction resolutions = %d, want 1", resolved)
	}
	if err := engine.CloseSession(created.ID); err != nil {
		t.Fatal(err)
	}
	if err := engine.Remove(context.Background(), created.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("existing workspace was removed: %v", err)
	}
}

func TestEngineUnsupportedOperations(t *testing.T) {
	engine, err := Open(context.Background(), t.TempDir(), WithAdapters(engineAdapter{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	created, err := engine.Start(context.Background(), StartRequest{ID: "unsupported", Backend: "test", WorkspaceMode: WorkspaceExisting, Worktree: engineRepo(t)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Restart(context.Background(), created.ID); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("Restart error = %T %v", err, err)
	}
	if _, err := engine.ForkPrompt(context.Background(), host.ForkPromptRequest{SessionID: created.ID, Prompt: "fork"}); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("ForkPrompt error = %T %v", err, err)
	}
	if err := engine.RespondInteraction(context.Background(), created.ID, host.InteractionResponse{RequestID: "request", Action: "cancel"}); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("RespondInteraction error = %T %v", err, err)
	}
}

func TestEngineQueueControlAndRestartFacades(t *testing.T) {
	adapter := &queueEngineAdapter{}
	engine, err := Open(context.Background(), t.TempDir(), WithAdapters(adapter))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	created, err := engine.Start(context.Background(), StartRequest{ID: "queue-control", Backend: "queue", WorkspaceMode: WorkspaceExisting, Worktree: engineRepo(t)})
	if err != nil {
		t.Fatal(err)
	}
	if _, sendErr := engine.Send(context.Background(), host.SendTurnRequest{SessionID: created.ID, Prompt: "active"}); sendErr != nil {
		t.Fatal(sendErr)
	}
	if blockErr := engine.BlockDispatch(created.ID); blockErr != nil {
		t.Fatal(blockErr)
	}
	queued, err := engine.Send(context.Background(), host.SendTurnRequest{SessionID: created.ID, Prompt: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	priority, err := engine.SendNext(context.Background(), host.SendTurnRequest{SessionID: created.ID, Prompt: "priority"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := engine.QueueEntries(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].ID != priority.QueueEntryID || entries[1].ID != queued.QueueEntryID {
		t.Fatalf("entries = %#v", entries)
	}
	replaced, err := engine.ReplaceQueuedTurn(created.ID, priority.QueueEntryID, host.SendTurnRequest{Prompt: "replacement"})
	if err != nil || replaced.ID != priority.QueueEntryID || replaced.Request.Prompt != "replacement" {
		t.Fatalf("replacement = %#v, %v", replaced, err)
	}
	removed, err := engine.RemoveQueuedTurn(created.ID, queued.QueueEntryID)
	if err != nil || !removed.Removed || removed.Entry.ID != queued.QueueEntryID {
		t.Fatalf("removed = %#v, %v", removed, err)
	}
	if interruptErr := engine.InterruptActive(context.Background(), created.ID); interruptErr != nil {
		t.Fatal(interruptErr)
	}
	if adapter.interruptCount() != 1 {
		t.Fatalf("interrupts = %d", adapter.interruptCount())
	}
	adapter.emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: "turn-1"})
	state, err := engine.Runtime().State(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.TurnActive || !state.DispatchBlocked || state.QueueDepth != 1 {
		t.Fatalf("blocked state = %#v", state)
	}
	if unblockErr := engine.UnblockDispatch(created.ID); unblockErr != nil {
		t.Fatal(unblockErr)
	}
	deadline := time.Now().Add(time.Second)
	for adapter.turnCount() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if prompts := adapter.prompts(); len(prompts) != 2 || prompts[1] != "replacement" {
		t.Fatalf("prompts = %#v", prompts)
	}
	adapter.emit(host.Event{Type: host.EventTurnComplete, BackendTurnID: "turn-2"})
	restarted, err := engine.Restart(context.Background(), created.ID)
	if err != nil || restarted.ID != "queue-provider-restarted" || adapter.restartCount() != 1 {
		t.Fatalf("restart = %#v, %v, calls = %d", restarted, err, adapter.restartCount())
	}
	persisted, err := engine.loadSession(created.ID)
	if err != nil || persisted.BackendSession.ID != restarted.ID {
		t.Fatalf("persisted restart = %#v, %v", persisted.BackendSession, err)
	}
}

func TestEngineHostJournalManifestAndSnapshot(t *testing.T) {
	home := t.TempDir()
	store, err := journal.NewStore(filepath.Join(home, "conversations"), journal.WithSchemaID("host.events.v1"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := &advancedEngineAdapter{}
	engine, err := Open(
		context.Background(), home,
		WithAdapters(adapter),
		WithJournalConfiguration(JournalConfiguration{Store: store, DisableRuntimeJournal: true}),
		WithRuntimeConfiguration(runtime.Config{MaxQueuedTurns: 3, DisableCoalescing: true}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ext := json.RawMessage(`{"example":{"workflow":"loop"}}`)
	created, err := engine.Start(context.Background(), StartRequest{
		ID: "host-state", Backend: "advanced", WorkspaceMode: WorkspaceExisting, Worktree: engineRepo(t),
		Model: "model-a", Reasoning: "high", PermissionMode: "approve", Ext: ext,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.UpdatedAt.Before(created.CreatedAt) || created.Configuration.Model != "model-a" || string(created.Ext) != string(ext) {
		t.Fatalf("manifest = %#v", created)
	}
	if _, statErr := os.Stat(filepath.Join(home, "journals")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("default journal directory exists: %v", statErr)
	}
	if _, appendErr := engine.Append(created.ID, "example.annotation", map[string]any{"value": true}, nil); appendErr != nil {
		t.Fatal(appendErr)
	}
	if _, sendErr := engine.Send(context.Background(), host.SendTurnRequest{SessionID: created.ID, Prompt: "active", Model: "model-b"}); sendErr != nil {
		t.Fatal(sendErr)
	}
	if blockErr := engine.BlockDispatch(created.ID); blockErr != nil {
		t.Fatal(blockErr)
	}
	if _, sendErr := engine.Send(context.Background(), host.SendTurnRequest{SessionID: created.ID, Prompt: "queued"}); sendErr != nil {
		t.Fatal(sendErr)
	}
	adapter.emit(host.Event{
		Type: host.EventInteractionRequested, BackendTurnID: "turn",
		Interaction: &host.InteractionRequest{ID: "choice", Kind: host.InteractionChoice, Options: []host.InteractionOption{{ID: "one"}}},
	})
	snapshot, err := engine.Snapshot(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Lifecycle.Status != "waiting_input" || !snapshot.ActiveTurn.Active || snapshot.ActiveTurn.ID != "turn" {
		t.Fatalf("lifecycle and turn = %#v %#v", snapshot.Lifecycle, snapshot.ActiveTurn)
	}
	if snapshot.Workspace.Mode != WorkspaceExisting || snapshot.Backend.Session.ID != "advanced-provider" {
		t.Fatalf("workspace and backend = %#v %#v", snapshot.Workspace, snapshot.Backend)
	}
	if snapshot.Queue.Depth != 1 || !snapshot.Queue.DispatchBlocked || len(snapshot.Queue.Entries) != 1 {
		t.Fatalf("queue = %#v", snapshot.Queue)
	}
	if snapshot.PendingInteraction == nil || snapshot.PendingInteraction.ID != "choice" {
		t.Fatalf("pending interaction = %#v", snapshot.PendingInteraction)
	}
	pending, err := engine.PendingInteraction(created.ID)
	if err != nil || pending == nil || pending.ID != "choice" {
		t.Fatalf("PendingInteraction = %#v, %v", pending, err)
	}
	pending.Options[0].ID = "changed"
	unchanged, err := engine.PendingInteraction(created.ID)
	if err != nil || unchanged.Options[0].ID != "one" {
		t.Fatalf("detached interaction = %#v, %v", unchanged, err)
	}
	if snapshot.Configuration.Model != "model-b" || snapshot.Configuration.Reasoning != "high" || snapshot.LastJournalSequence != 1 || string(snapshot.Ext) != string(ext) {
		t.Fatalf("configuration and host data = %#v", snapshot)
	}
	if respondErr := engine.RespondInteraction(context.Background(), created.ID, host.InteractionResponse{RequestID: "choice", Action: "submit"}); respondErr != nil {
		t.Fatal(respondErr)
	}
	resolved, err := engine.Snapshot(created.ID)
	if err != nil || resolved.PendingInteraction != nil {
		t.Fatalf("resolved interaction snapshot = %#v, %v", resolved.PendingInteraction, err)
	}
	adapter.emit(host.Event{Type: host.EventModels, Data: map[string]any{"current_model": "model-canonical"}})
	adapter.emit(host.Event{Type: host.EventReasoningLevels, Data: map[string]any{"current_reasoning": "medium"}})
	adapter.emit(host.Event{Type: host.EventPermissionModes, Data: map[string]any{"current_mode": "manual"}})
	configured, err := engine.Snapshot(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if configured.Configuration != (Configuration{Model: "model-canonical", Reasoning: "medium", PermissionMode: "manual"}) {
		t.Fatalf("provider configuration = %#v", configured.Configuration)
	}
	persisted, err := engine.loadSession(created.ID)
	if err != nil || persisted.Configuration != configured.Configuration {
		t.Fatalf("persisted provider configuration = %#v, %v", persisted.Configuration, err)
	}
	records, err := engine.History(created.ID, 0, 0)
	if err != nil || len(records) != 1 || records[0].Event != "example.annotation" {
		t.Fatalf("History = %#v, %v", records, err)
	}
	if records, err = engine.HistoryAfter(created.ID, records[0].Sequence); err != nil || len(records) != 0 {
		t.Fatalf("HistoryAfter = %#v, %v", records, err)
	}
	if records, err = engine.HistoryTail(created.ID, 1); err != nil || len(records) != 1 {
		t.Fatalf("HistoryTail = %#v, %v", records, err)
	}
	engine.SetCatalog("advanced", host.BackendCatalog{Models: []host.BackendModel{{ID: "model"}}})
	if catalog := engine.Catalog(context.Background(), false); len(catalog["advanced"].Models) != 1 {
		t.Fatalf("Catalog = %#v", catalog)
	}
	if len(engine.Detect(context.Background())) == 0 {
		t.Fatal("Detect returned no adapters")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if record, err := store.Append(journal.Record{SessionID: created.ID, Event: "example.after_close"}); err != nil || record.Sequence != 2 {
		t.Fatalf("caller-owned store after Engine close = %#v, %v", record, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEngineValidationConfigurationAndHelpers(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(cancelled, t.TempDir()); err == nil {
		t.Fatal("Open accepted a cancelled context")
	}
	if _, err := Open(context.Background(), ""); err == nil {
		t.Fatal("Open accepted an empty home")
	}
	fileHome := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileHome, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), fileHome); err == nil {
		t.Fatal("Open accepted a file as home")
	}
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, configFileName), []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), home); err == nil {
		t.Fatal("Open accepted unsupported config version")
	}
	if err := os.WriteFile(filepath.Join(home, configFileName), []byte(`not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), home); err == nil {
		t.Fatal("Open accepted malformed config")
	}
	engine, err := Open(context.Background(), t.TempDir(), WithAdapters(engineAdapter{}), WithBranchPrefix(" /host/ "))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if engine.Home() == "" || engine.Settings().Worktrees.BranchPrefix != "host" || engine.Runtime() == nil || engine.Journal() == nil {
		t.Fatalf("engine accessors = %#v", engine.Settings())
	}
	unsubscribe := engine.Subscribe(func(host.Event) { t.Fatal("unsubscribed callback invoked") })
	unsubscribe()
	engine.publish(host.Event{Type: host.EventMessage})
	if _, err := engine.Start(context.Background(), StartRequest{}); err == nil {
		t.Fatal("Start accepted missing backend")
	}
	if _, err := engine.Start(context.Background(), StartRequest{ID: "bad/id", Backend: "test", Source: engineRepo(t)}); err == nil {
		t.Fatal("Start accepted unsafe ID")
	}
	if _, err := engine.Start(context.Background(), StartRequest{ID: "wrong-mode", Backend: "test", WorkspaceMode: "wrong"}); err == nil {
		t.Fatal("Start accepted unsupported workspace mode")
	}
	if _, err := engine.Append("", "annotation", nil, nil); err == nil {
		t.Fatal("Append accepted invalid extension event")
	}
	if _, err := engine.loadSession("missing"); err == nil {
		t.Fatal("loadSession accepted missing manifest")
	}
	if err := os.WriteFile(engine.sessionPath("corrupt"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.loadSession("corrupt"); err == nil {
		t.Fatal("loadSession accepted invalid manifest")
	}
	for raw, want := range map[string]string{"hello world": "hello-world", "": "session", "../x": "---x"} {
		if got := safeFileName(raw); got != want {
			t.Fatalf("safeFileName(%q) = %q, want %q", raw, got, want)
		}
	}
	if validSessionID("bad/id") || !validSessionID("good-id_1") || validSessionID("") {
		t.Fatal("validSessionID returned unexpected result")
	}
	settings := Settings{}
	if err := normalizeSettings(&settings); err != nil || settings.Worktrees.BranchPrefix == "" {
		t.Fatalf("normalize settings = %#v, %v", settings, err)
	}
	if err := normalizeSettings(nil); err == nil {
		t.Fatal("nil settings normalized")
	}
	if got := catalogDir(home, Settings{CatalogCache: false}); got != "" {
		t.Fatalf("disabled catalog dir = %q", got)
	}
	if err := writePrivate(filepath.Join(home, "private", "value"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	privatePath := filepath.Join(home, "private", "value")
	// #nosec G304 -- Test reads the fixed path it created below a test-owned directory.
	if raw, err := os.ReadFile(privatePath); err != nil || string(raw) != "ok" {
		t.Fatalf("private file = %q, %v", raw, err)
	}
	if _, err := sessionID(); err != nil {
		t.Fatal(err)
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestEngineSessionListingAndManifestRoundTrip(t *testing.T) {
	home := t.TempDir()
	engine, err := Open(context.Background(), home, WithAdapters(engineAdapter{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	for _, id := range []string{"a", "b"} {
		if _, err := engine.Start(context.Background(), StartRequest{ID: id, Backend: "test", WorkspaceMode: WorkspaceExisting, Worktree: engineRepo(t)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(home, "sessions", "ignored.txt"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := engine.Sessions()
	if err != nil || len(entries) != 2 || entries[0].ID != "a" || entries[1].ID != "b" {
		t.Fatalf("sessions = %#v, %v", entries, err)
	}
	entry := entries[0]
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(raw) {
		t.Fatal("session manifest did not marshal")
	}
	if err := engine.CloseSession("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Resume(context.Background(), "a"); err == nil {
		t.Fatal("Resume accepted closed session")
	}
	if err := engine.Remove(context.Background(), "b", false); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.loadSession("b"); err == nil {
		t.Fatal("Remove retained manifest")
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestEngineRepairPruneRollbackAndNilGuards(t *testing.T) {
	if (*Engine)(nil).Home() != "" || (*Engine)(nil).Runtime() != nil || (*Engine)(nil).Journal() != nil {
		t.Fatal("nil Engine accessors returned state")
	}
	if _, err := (*Engine)(nil).Sessions(); err == nil {
		t.Fatal("nil Engine listed sessions")
	}
	if _, err := (*Engine)(nil).Send(context.Background(), host.SendTurnRequest{}); err == nil {
		t.Fatal("nil Engine sent a turn")
	}
	if _, err := (*Engine)(nil).SendNext(context.Background(), host.SendTurnRequest{}); err == nil {
		t.Fatal("nil Engine prepended a turn")
	}
	if _, err := (*Engine)(nil).Snapshot("s"); err == nil {
		t.Fatal("nil Engine returned a snapshot")
	}
	if err := (*Engine)(nil).SetConfiguration("s", Configuration{}); err == nil {
		t.Fatal("nil Engine set configuration")
	}
	if _, err := (*Engine)(nil).QueueEntries("s"); err == nil {
		t.Fatal("nil Engine listed queued turns")
	}
	if _, err := (*Engine)(nil).RemoveQueuedTurn("s", "q"); err == nil {
		t.Fatal("nil Engine removed a queued turn")
	}
	if _, err := (*Engine)(nil).ReplaceQueuedTurn("s", "q", host.SendTurnRequest{}); err == nil {
		t.Fatal("nil Engine replaced a queued turn")
	}
	if err := (*Engine)(nil).BlockDispatch("s"); err == nil {
		t.Fatal("nil Engine blocked dispatch")
	}
	if err := (*Engine)(nil).UnblockDispatch("s"); err == nil {
		t.Fatal("nil Engine unblocked dispatch")
	}
	if _, err := (*Engine)(nil).Restart(context.Background(), "s"); err == nil {
		t.Fatal("nil Engine restarted a session")
	}
	if err := (*Engine)(nil).Interrupt(context.Background(), "s"); err == nil {
		t.Fatal("nil Engine interrupted a session")
	}
	if err := (*Engine)(nil).InterruptActive(context.Background(), "s"); err == nil {
		t.Fatal("nil Engine interrupted an active turn")
	}
	if err := (*Engine)(nil).RespondInteraction(context.Background(), "s", host.InteractionResponse{}); err == nil {
		t.Fatal("nil Engine responded to an interaction")
	}
	if _, err := (*Engine)(nil).ForkPrompt(context.Background(), host.ForkPromptRequest{}); err == nil {
		t.Fatal("nil Engine forked a prompt")
	}
	if _, err := (*Engine)(nil).Append("s", "host.note", nil, nil); err == nil {
		t.Fatal("nil Engine appended a record")
	}
	if err := (*Engine)(nil).Close(); err != nil {
		t.Fatal(err)
	}

	source := engineRepo(t)
	engine, err := Open(context.Background(), t.TempDir(), WithAdapters(engineAdapter{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if _, err := engine.Start(context.Background(), StartRequest{ID: "missing-provider", Backend: "missing", Source: source}); err == nil {
		t.Fatal("Start accepted missing provider")
	}
	if _, err := os.Stat(filepath.Join(engine.Home(), "sessions", "missing-provider.json")); !os.IsNotExist(err) {
		t.Fatalf("failed Start retained manifest: %v", err)
	}
	if _, err := engine.Start(context.Background(), StartRequest{ID: "missing-existing", Backend: "test", WorkspaceMode: WorkspaceExisting, Worktree: filepath.Join(source, "missing")}); err == nil {
		t.Fatal("Start accepted absent existing workspace")
	}
	created, err := engine.Start(context.Background(), StartRequest{ID: "repair", Backend: "test", WorkspaceMode: WorkspaceExisting, Worktree: source})
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := engine.Repair(context.Background(), created.ID)
	if err != nil || repaired.Worktree.Path == "" {
		t.Fatalf("Repair = %#v, %v", repaired, err)
	}
	if err := engine.Prune(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if err := engine.Prune(context.Background(), t.TempDir()); err == nil {
		t.Fatal("Prune accepted non-repository")
	}
	if err := engine.Close(); err != nil || engine.Close() != nil {
		t.Fatalf("Close was not idempotent: %v", err)
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestEnginePersistenceHelpers(t *testing.T) {
	home := t.TempDir()
	defaults, err := loadSettings(home)
	if err != nil || defaults.Version != 1 {
		t.Fatalf("default settings = %#v, %v", defaults, err)
	}
	settings := Settings{Version: 1, Worktrees: WorktreeSettings{BranchPrefix: " custom "}, CatalogCache: true}
	if err := saveSettings(home, settings); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadSettings(home)
	if err != nil || loaded.Worktrees.BranchPrefix != "custom" {
		t.Fatalf("loaded settings = %#v, %v", loaded, err)
	}
	if err := os.WriteFile(filepath.Join(home, configFileName), []byte(`[]`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSettings(home); err == nil {
		t.Fatal("loadSettings accepted wrong JSON shape")
	}
	if err := secureDir(filepath.Join(home, "secure")); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "..", "valid", "also.valid"} {
		_ = safeFileName(value)
	}
	merged := mergeAdapters([]host.Adapter{nil, engineAdapter{}, duplicateEngineAdapter{}})
	if len(merged) != 1 || merged[0].Backend() != "test" {
		t.Fatalf("merged adapters = %#v", merged)
	}
}

type engineAdapter struct{}

type duplicateEngineAdapter struct{ engineAdapter }

func (engineAdapter) Backend() host.Backend { return "test" }

func (engineAdapter) Detect(context.Context) host.BackendStatus {
	return host.BackendStatus{Backend: "test", Available: true}
}

func (engineAdapter) StartSession(context.Context, string, host.StartSessionRequest, host.EventSink) (host.BackendSession, error) {
	return host.BackendSession{ID: "provider-session"}, nil
}

func (engineAdapter) SendTurn(context.Context, string, host.SendTurnRequest, host.EventSink) (host.BackendSession, error) {
	return host.BackendSession{ID: "provider-session", TurnID: "turn-1"}, nil
}

func (engineAdapter) Interrupt(context.Context, string, host.EventSink) error { return nil }

func (engineAdapter) CloseSession(string) error { return nil }

type advancedEngineAdapter struct {
	sink       host.EventSink
	answer     host.InteractionResponse
	interrupts int
}

type queueEngineAdapter struct {
	mu         sync.Mutex
	sink       host.EventSink
	turns      []string
	interrupts int
	restarts   int
}

func (*queueEngineAdapter) Backend() host.Backend { return "queue" }

func (*queueEngineAdapter) Detect(context.Context) host.BackendStatus {
	return host.BackendStatus{Backend: "queue", Available: true}
}

func (a *queueEngineAdapter) StartSession(_ context.Context, _ string, _ host.StartSessionRequest, sink host.EventSink) (host.BackendSession, error) {
	a.mu.Lock()
	a.sink = sink
	a.mu.Unlock()
	return host.BackendSession{ID: "queue-provider"}, nil
}

func (a *queueEngineAdapter) SendTurn(_ context.Context, _ string, request host.SendTurnRequest, sink host.EventSink) (host.BackendSession, error) {
	a.mu.Lock()
	a.sink = sink
	a.turns = append(a.turns, request.Prompt)
	turnID := fmt.Sprintf("turn-%d", len(a.turns))
	a.mu.Unlock()
	sink(host.Event{Type: host.EventTurnStarted, BackendTurnID: turnID})
	return host.BackendSession{ID: "queue-provider", TurnID: turnID}, nil
}

func (a *queueEngineAdapter) Interrupt(context.Context, string, host.EventSink) error {
	a.mu.Lock()
	a.interrupts++
	a.mu.Unlock()
	return nil
}

func (*queueEngineAdapter) CloseSession(string) error { return nil }

func (a *queueEngineAdapter) RestartSession(context.Context, string, host.EventSink) (host.BackendSession, error) {
	a.mu.Lock()
	a.restarts++
	a.mu.Unlock()
	return host.BackendSession{ID: "queue-provider-restarted"}, nil
}

func (a *queueEngineAdapter) emit(event host.Event) {
	a.mu.Lock()
	sink := a.sink
	a.mu.Unlock()
	if sink != nil {
		sink(event)
	}
}

func (a *queueEngineAdapter) prompts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.turns...)
}

func (a *queueEngineAdapter) turnCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.turns)
}

func (a *queueEngineAdapter) interruptCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.interrupts
}

func (a *queueEngineAdapter) restartCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.restarts
}

func (*advancedEngineAdapter) Backend() host.Backend { return "advanced" }

func (*advancedEngineAdapter) Detect(context.Context) host.BackendStatus {
	return host.BackendStatus{Backend: "advanced", Available: true}
}

func (a *advancedEngineAdapter) StartSession(_ context.Context, _ string, request host.StartSessionRequest, sink host.EventSink) (host.BackendSession, error) {
	a.sink = sink
	state := host.BackendSession{ID: "advanced-provider"}
	if request.Prompt != "" || len(request.Attachments) > 0 {
		state.TurnID = "initial-turn"
		sink(host.Event{Type: host.EventTurnStarted, BackendTurnID: state.TurnID})
		sink(host.Event{Type: host.EventTurnComplete, BackendTurnID: state.TurnID})
	}
	return state, nil
}

func (a *advancedEngineAdapter) SendTurn(_ context.Context, _ string, _ host.SendTurnRequest, sink host.EventSink) (host.BackendSession, error) {
	a.sink = sink
	if sink != nil {
		sink(host.Event{Type: host.EventTurnStarted, BackendTurnID: "turn"})
	}
	return host.BackendSession{ID: "advanced-provider", TurnID: "turn"}, nil
}

func (a *advancedEngineAdapter) Interrupt(_ context.Context, _ string, _ host.EventSink) error {
	a.interrupts++
	return nil
}

func (*advancedEngineAdapter) CloseSession(string) error { return nil }

func (a *advancedEngineAdapter) RespondInteraction(_ context.Context, _ string, response host.InteractionResponse) error {
	a.answer = response
	if a.sink != nil {
		a.sink(host.Event{Type: host.EventInteractionResolved, InteractionResponse: &response})
	}
	return nil
}

func (*advancedEngineAdapter) ForkPrompt(_ context.Context, request host.ForkPromptRequest) (host.ForkPromptResponse, error) {
	return host.ForkPromptResponse{Accepted: request.SessionID != ""}, nil
}

func (a *advancedEngineAdapter) emit(event host.Event) {
	if a.sink != nil {
		a.sink(event)
	}
}

func engineRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "--initial-branch=main"},
		{"config", "user.email", "test@example.com"},
		{"config", "user.name", "Test User"},
	} {
		if err := engineGit(dir, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := engineGit(dir, "add", "README.md"); err != nil {
		t.Fatal(err)
	}
	if err := engineGit(dir, "commit", "-m", "initial"); err != nil {
		t.Fatal(err)
	}
	return dir
}

func engineGit(directory string, args ...string) error {
	// #nosec G204 -- Test helper invokes the fixed Git binary in a temp repository.
	return exec.CommandContext(context.Background(), "git", append([]string{"-C", directory}, args...)...).Run()
}
