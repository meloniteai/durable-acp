//go:build realacp

package realacp_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	durableacp "github.com/meloniteai/durable-acp"
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/adapters/antigravity"
	"github.com/meloniteai/durable-acp/adapters/claude"
	"github.com/meloniteai/durable-acp/adapters/codex"
	"github.com/meloniteai/durable-acp/adapters/cursor"
	"github.com/meloniteai/durable-acp/host"
	"github.com/meloniteai/durable-acp/journal"
	"github.com/meloniteai/durable-acp/runtime"
)

const (
	realModelEnv          = "DURABLE_ACP_REAL_MODEL"
	realReasoningEnv      = "DURABLE_ACP_REAL_REASONING"
	realPermissionModeEnv = "DURABLE_ACP_REAL_PERMISSION_MODE"
	realTestTimeout       = 6 * time.Minute
)

// Each journey is intentionally an independent Go test. The runner installs
// the ACPs and coding CLIs once, then executes these model-bound journeys in
// parallel isolated workers. This catches state leakage that a serial,
// all-in-one scenario would hide.
func TestRealACPCodexManagedLifecycle(t *testing.T)  { runManagedLifecycle(t, codex.Backend) }
func TestRealACPClaudeManagedLifecycle(t *testing.T) { runManagedLifecycle(t, claude.Backend) }
func TestRealACPCursorManagedLifecycle(t *testing.T) { runManagedLifecycle(t, cursor.Backend) }
func TestRealACPAntigravityManagedLifecycle(t *testing.T) {
	runManagedLifecycle(t, antigravity.Backend)
}

func TestRealACPCodexExistingWorkspaceAttachment(t *testing.T) {
	runExistingWorkspaceAttachment(t, codex.Backend)
}
func TestRealACPClaudeExistingWorkspaceAttachment(t *testing.T) {
	runExistingWorkspaceAttachment(t, claude.Backend)
}
func TestRealACPCursorExistingWorkspaceAttachment(t *testing.T) {
	runExistingWorkspaceAttachment(t, cursor.Backend)
}
func TestRealACPAntigravityExistingWorkspaceAttachment(t *testing.T) {
	runExistingWorkspaceAttachment(t, antigravity.Backend)
}

func TestRealACPCodexQueuedTurns(t *testing.T)       { runQueuedTurns(t, codex.Backend) }
func TestRealACPClaudeQueuedTurns(t *testing.T)      { runQueuedTurns(t, claude.Backend) }
func TestRealACPCursorQueuedTurns(t *testing.T)      { runQueuedTurns(t, cursor.Backend) }
func TestRealACPAntigravityQueuedTurns(t *testing.T) { runQueuedTurns(t, antigravity.Backend) }

func TestRealACPCodexInterruptAndRecovery(t *testing.T) {
	runInterruptAndRecovery(t, codex.Backend)
}
func TestRealACPClaudeInterruptAndRecovery(t *testing.T) {
	runInterruptAndRecovery(t, claude.Backend)
}
func TestRealACPCursorInterruptAndRecovery(t *testing.T) {
	runInterruptAndRecovery(t, cursor.Backend)
}
func TestRealACPAntigravityInterruptAndRecovery(t *testing.T) {
	runInterruptAndRecovery(t, antigravity.Backend)
}

func TestRealACPCodexPermissionRoundTrip(t *testing.T) {
	runPermissionRoundTrip(t, codex.Backend)
}
func TestRealACPClaudePermissionRoundTrip(t *testing.T) {
	runPermissionRoundTrip(t, claude.Backend)
}
func TestRealACPCursorPermissionRoundTrip(t *testing.T) {
	runPermissionRoundTrip(t, cursor.Backend)
}
func TestRealACPAntigravityPermissionMode(t *testing.T) {
	runNativePlanMode(t, antigravity.Backend)
}

// runManagedLifecycle covers the state which only a real provider can prove:
// setup/configuration, live model file edits, normalized stream/journal data,
// worktree repair, process restart + provider resume, and owned cleanup.
func runManagedLifecycle(t *testing.T, backend host.Backend) {
	t.Helper()
	ctx, cancel := realContext(t)
	defer cancel()
	live := newLiveEngine(t, ctx, backend)
	assertDiscovery(t, ctx, live, backend)

	source := newGitRepository(t)
	session := live.start(t, ctx, durableacp.StartRequest{
		ID:             "managed-lifecycle",
		Backend:        backend,
		WorkspaceMode:  durableacp.WorkspaceManaged,
		Source:         source,
		Model:          modelFor(backend),
		Reasoning:      reasoningFor(backend),
		PermissionMode: writablePermissionModeFor(backend),
	})
	assertManagedWorktree(t, session)
	assertSessionListed(t, live.engineValue(t), session.ID, false)

	proof := "durable-acp-managed-" + string(backend)
	before := live.events.len()
	result, err := live.send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt: "Use your file-editing or shell tools to create durable-acp-managed.txt in the " +
			"repository root containing exactly this text and nothing else: " + proof + ". " +
			"Do not modify any other file. When done, reply exactly done.",
	})
	if err != nil {
		t.Fatalf("send managed edit: %v", err)
	}
	if !result.Accepted || result.Queued {
		t.Fatalf("managed turn result = %+v, want immediate accepted turn", result)
	}
	assertTurnSucceeded(t, ctx, live.events, before)
	stream := live.events.after(before)
	assertLiveEditingStream(t, stream)
	assertTurnIdentities(t, stream, 1)
	t.Logf("live %s managed stream: %s", backend, eventSummary(stream))
	assertFileText(t, filepath.Join(session.Worktree.Path, "durable-acp-managed.txt"), proof)
	engine := live.engineValue(t)
	restarted, err := engine.Restart(ctx, session.ID)
	if err != nil {
		t.Fatalf("restart live provider session: %v", err)
	}
	if restarted.ID == "" || restarted.ID != session.BackendSession.ID {
		t.Fatalf("restarted backend identity = %+v, want original %+v", restarted, session.BackendSession)
	}
	runtimeState, err := engine.Runtime().State(session.ID)
	if err != nil {
		t.Fatalf("read restarted runtime state: %v", err)
	}
	if runtimeState.TurnActive || runtimeState.Session.Status == "closed" {
		t.Fatalf("restarted runtime state = %+v", runtimeState)
	}
	restartProof := "durable-acp-restarted-" + string(backend)
	before = live.events.len()
	if _, err := live.send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt: "Use a tool to create durable-acp-restarted.txt containing exactly " + restartProof +
			" and nothing else. Do not modify another file. Reply exactly done.",
	}); err != nil {
		t.Fatalf("send turn after live restart: %v", err)
	}
	assertTurnSucceeded(t, ctx, live.events, before)
	assertTurnIdentities(t, live.events.after(before), 1)
	assertFileText(t, filepath.Join(session.Worktree.Path, "durable-acp-restarted.txt"), restartProof)

	presentation := &journal.Presentation{Label: "Live verification"}
	if _, err := live.engineValue(t).Append(session.ID, "example.live_verified", map[string]any{
		"backend": backend,
		"proof":   proof,
	}, presentation); err != nil {
		t.Fatalf("append opaque journal record: %v", err)
	}
	records, err := live.engineValue(t).Journal().Read(session.ID, 0, 0)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	assertJournal(t, records, "agent.message", "agent.turn_started", "agent.yielded", "example.live_verified")

	repaired, err := live.engineValue(t).Repair(ctx, session.ID)
	if err != nil {
		t.Fatalf("repair managed worktree: %v", err)
	}
	if repaired.Worktree.Path != session.Worktree.Path || repaired.Worktree.Branch != session.Worktree.Branch {
		t.Fatalf("repair changed managed identity: %+v", repaired.Worktree)
	}
	if err := live.engineValue(t).Prune(ctx, source); err != nil {
		t.Fatalf("prune managed source: %v", err)
	}

	live.reopen(t, ctx)
	resumed, err := live.engineValue(t).Resume(ctx, session.ID)
	if err != nil {
		t.Fatalf("resume provider session after Engine restart: %v", err)
	}
	if resumed.BackendSession.ID == "" || resumed.BackendSession.ID != session.BackendSession.ID {
		t.Fatalf("resumed backend identity = %+v, want original %+v", resumed.BackendSession, session.BackendSession)
	}
	resumeProof := "durable-acp-resumed-" + string(backend)
	before = live.events.len()
	if _, err := live.send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt: "Use a tool to create durable-acp-resumed.txt containing exactly " + resumeProof +
			" and nothing else. Do not modify another file. Reply exactly done.",
	}); err != nil {
		t.Fatalf("send resumed turn: %v", err)
	}
	assertTurnSucceeded(t, ctx, live.events, before)
	assertTurnIdentities(t, live.events.after(before), 1)
	assertFileText(t, filepath.Join(session.Worktree.Path, "durable-acp-resumed.txt"), resumeProof)

	engine = live.engineValue(t)
	if err := engine.CloseSession(session.ID); err != nil {
		t.Fatalf("close session: %v", err)
	}
	assertSessionListed(t, engine, session.ID, true)
	if err := engine.Remove(ctx, session.ID, false); err == nil {
		t.Fatal("remove modified managed session without force unexpectedly succeeded")
	}
	if _, statErr := os.Stat(session.Worktree.Path); statErr != nil {
		t.Fatalf("guarded remove changed managed worktree: %v", statErr)
	}
	if err := engine.Remove(ctx, session.ID, true); err != nil {
		t.Fatalf("force-remove managed session: %v", err)
	}
	if _, statErr := os.Stat(session.Worktree.Path); !os.IsNotExist(statErr) {
		t.Fatalf("removed managed worktree stat error = %v, want not exist", statErr)
	}
	assertSessionAbsent(t, engine, session.ID)
}

// runExistingWorkspaceAttachment uses Start's initial turn (rather than Send)
// with an out-of-workspace resource attachment. It proves the embedded host
// can leave a caller-owned checkout untouched while passing ACP content blocks.
func runExistingWorkspaceAttachment(t *testing.T, backend host.Backend) {
	t.Helper()
	ctx, cancel := realContext(t)
	defer cancel()
	live := newLiveEngine(t, ctx, backend)
	source := newGitRepository(t)
	attachment := filepath.Join(t.TempDir(), "attachment.txt")
	proof := "durable-acp-attachment-" + string(backend)
	if err := os.WriteFile(attachment, []byte(proof+"\n"), 0o600); err != nil {
		t.Fatalf("write attached resource: %v", err)
	}

	before := live.events.len()
	session := live.start(t, ctx, durableacp.StartRequest{
		ID:            "existing-attachment",
		Backend:       backend,
		WorkspaceMode: durableacp.WorkspaceExisting,
		Worktree:      source,
		Prompt: "Read the attached text resource. Its complete content is a token. " +
			"Use your tools to create durable-acp-attachment.txt in the repository root " +
			"containing exactly that token and nothing else. Do not modify any other file. " +
			"Reply exactly done.",
		Attachments: []host.Attachment{{
			Name:     "realacp-token.txt",
			MimeType: "text/plain",
			Path:     attachment,
		}},
		Model:          modelFor(backend),
		Reasoning:      reasoningFor(backend),
		PermissionMode: writablePermissionModeFor(backend),
	})
	if session.WorkspaceMode != durableacp.WorkspaceExisting {
		t.Fatalf("existing workspace session = %+v", session)
	}
	assertSameDirectory(t, session.Worktree.Path, source)
	assertTurnSucceeded(t, ctx, live.events, before)
	assertTurnIdentities(t, live.events.after(before), 1)
	assertFileText(t, filepath.Join(source, "durable-acp-attachment.txt"), proof)

	engine := live.engineValue(t)
	if _, err := engine.Repair(ctx, session.ID); err != nil {
		t.Fatalf("repair existing workspace: %v", err)
	}
	if err := engine.CloseSession(session.ID); err != nil {
		t.Fatalf("close existing workspace session: %v", err)
	}
	if err := engine.Remove(ctx, session.ID, false); err != nil {
		t.Fatalf("remove existing workspace session: %v", err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("Remove deleted caller-owned workspace: %v", err)
	}
	assertFileText(t, filepath.Join(source, "durable-acp-attachment.txt"), proof)
}

// runQueuedTurns proves that two live model requests are serialized by the
// runtime rather than concurrently reaching a provider session.
func runQueuedTurns(t *testing.T, backend host.Backend) {
	t.Helper()
	ctx, cancel := realContext(t)
	defer cancel()
	live := newLiveEngine(t, ctx, backend)
	session := live.start(t, ctx, durableacp.StartRequest{
		ID:             "queued-turns",
		Backend:        backend,
		WorkspaceMode:  durableacp.WorkspaceManaged,
		Source:         newGitRepository(t),
		Model:          modelFor(backend),
		Reasoning:      reasoningFor(backend),
		PermissionMode: writablePermissionModeFor(backend),
	})

	firstProof := "durable-acp-first-" + string(backend)
	firstStarted := live.events.count(host.EventTurnStarted)
	firstDone := make(chan sendOutcome, 1)
	go func() {
		result, err := live.send(ctx, host.SendTurnRequest{
			SessionID: session.ID,
			Prompt: "Use a tool to create durable-acp-first.txt containing exactly " + firstProof +
				" and nothing else. Do not modify another file. Reply exactly done.",
		})
		firstDone <- sendOutcome{result: result, err: err}
	}()
	if err := live.events.wait(ctx, "first queued turn to start", func(events []host.Event) bool {
		return countEvents(events, host.EventTurnStarted) > firstStarted
	}); err != nil {
		t.Fatal(err)
	}
	engine := live.engineValue(t)
	state, err := engine.Runtime().State(session.ID)
	if err != nil {
		t.Fatalf("read active runtime state: %v", err)
	}
	if !state.TurnActive || state.ActiveTurnID == "" {
		t.Fatalf("active runtime state = %+v", state)
	}
	if err := engine.BlockDispatch(session.ID); err != nil {
		t.Fatalf("block queued dispatch: %v", err)
	}

	secondProof := "durable-acp-second-" + string(backend)
	queued, err := live.send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt: "Read durable-acp-first.txt with a tool. Then create durable-acp-second.txt " +
			"containing exactly " + secondProof + " and nothing else. Do not modify another file. Reply exactly done.",
	})
	if err != nil {
		t.Fatalf("queue second turn: %v", err)
	}
	if !queued.Accepted || !queued.Queued || queued.QueueDepth != 1 {
		t.Fatalf("queued turn = %+v, want accepted queue depth one", queued)
	}
	removed, err := live.send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt:    "Create durable-acp-removed.txt containing should-not-run. Reply exactly done.",
	})
	if err != nil {
		t.Fatalf("queue removable turn: %v", err)
	}
	if !removed.Queued || removed.QueueDepth != 2 || removed.QueueEntryID == "" || removed.QueueEntryID == queued.QueueEntryID {
		t.Fatalf("removable queued turn = %+v", removed)
	}
	entries, err := engine.QueueEntries(session.ID)
	if err != nil {
		t.Fatalf("list queued turns: %v", err)
	}
	if len(entries) != 2 || entries[0].ID != queued.QueueEntryID || entries[1].ID != removed.QueueEntryID {
		t.Fatalf("queued entries = %+v", entries)
	}
	removedResult, err := engine.RemoveQueuedTurn(session.ID, removed.QueueEntryID)
	if err != nil {
		t.Fatalf("remove queued turn: %v", err)
	}
	if !removedResult.Removed || removedResult.Entry.ID != removed.QueueEntryID || removedResult.QueueDepth != 1 {
		t.Fatalf("removed queued turn = %+v", removedResult)
	}
	first := <-firstDone
	if first.err != nil || !first.result.Accepted || first.result.Queued {
		t.Fatalf("first turn = %+v, %v", first.result, first.err)
	}
	state, err = engine.Runtime().State(session.ID)
	if err != nil {
		t.Fatalf("read blocked runtime state: %v", err)
	}
	if state.TurnActive || !state.DispatchBlocked || state.QueueDepth != 1 || len(state.QueueEntries) != 1 || state.QueueEntries[0].ID != queued.QueueEntryID {
		t.Fatalf("blocked runtime state = %+v", state)
	}
	if live.events.count(host.EventTurnStarted) != firstStarted+1 {
		t.Fatalf("queued turn dispatched through barrier: %s", eventSummary(live.events.snapshot()))
	}
	if err := engine.UnblockDispatch(session.ID); err != nil {
		t.Fatalf("unblock queued dispatch: %v", err)
	}
	if err := live.events.wait(ctx, "both queued turns to complete", func(events []host.Event) bool {
		return countEvents(events, host.EventTurnComplete) >= 2
	}); err != nil {
		t.Fatal(err)
	}
	assertNoTurnFailure(t, live.events.snapshot())
	assertFileText(t, filepath.Join(session.Worktree.Path, "durable-acp-first.txt"), firstProof)
	assertFileText(t, filepath.Join(session.Worktree.Path, "durable-acp-second.txt"), secondProof)
	if _, err := os.Stat(filepath.Join(session.Worktree.Path, "durable-acp-removed.txt")); !os.IsNotExist(err) {
		t.Fatalf("removed queued turn ran: stat error = %v", err)
	}
	assertTurnIdentities(t, live.events.snapshot(), 2)
	assertQueueTransition(t, live.events.snapshot(), 1, 2, 0)
	closeAndRemove(t, ctx, live.engineValue(t), session)
}

// runInterruptAndRecovery starts an actual long-running agent tool call,
// clears a queued follow-up, then proves the provider session accepts a new
// live model turn after cancellation.
func runInterruptAndRecovery(t *testing.T, backend host.Backend) {
	t.Helper()
	ctx, cancel := realContext(t)
	defer cancel()
	live := newLiveEngine(t, ctx, backend)
	session := live.start(t, ctx, durableacp.StartRequest{
		ID:             "interrupt-recovery",
		Backend:        backend,
		WorkspaceMode:  durableacp.WorkspaceManaged,
		Source:         newGitRepository(t),
		Model:          modelFor(backend),
		Reasoning:      reasoningFor(backend),
		PermissionMode: writablePermissionModeFor(backend),
	})

	firstBefore := live.events.len()
	turnsBefore := live.events.count(host.EventTurnStarted)
	toolsBefore := live.events.count(host.EventToolStarted)
	firstDone := make(chan sendOutcome, 1)
	go func() {
		result, err := live.send(ctx, host.SendTurnRequest{
			SessionID: session.ID,
			Prompt: "Use a shell tool immediately to run `sleep 30`. Do not answer before the " +
				"command finishes and do not modify files.",
		})
		firstDone <- sendOutcome{result: result, err: err}
	}()
	if err := live.events.wait(ctx, "the live long-running turn to start", func(events []host.Event) bool {
		return countEvents(events, host.EventTurnStarted) > turnsBefore
	}); err != nil {
		t.Fatal(err)
	}
	// EventTurnStarted is emitted before the ACP prompt reaches the provider.
	// Waiting for a tool call proves there is an active provider operation to
	// cancel and avoids racing ACPs which do not yet have a backend turn ID.
	if err := live.events.wait(ctx, "the live long-running tool call to start", func(events []host.Event) bool {
		return countEvents(events, host.EventToolStarted) > toolsBefore
	}); err != nil {
		t.Fatal(err)
	}
	queued, err := live.send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt:    "Create should-not-exist.txt containing cancelled. Reply exactly done.",
	})
	if err != nil {
		t.Fatalf("queue turn before interrupt: %v", err)
	}
	if !queued.Queued || queued.QueueDepth != 1 {
		t.Fatalf("queued turn before interrupt = %+v", queued)
	}
	if err := live.engineValue(t).Interrupt(ctx, session.ID); err != nil {
		t.Fatalf("interrupt active turn: %v", err)
	}
	select {
	case <-firstDone:
	case <-time.After(45 * time.Second):
		t.Fatal("real ACP did not return from interrupted prompt")
	}
	if err := live.events.wait(ctx, "queue to clear after interrupt", func(events []host.Event) bool {
		return queueDepth(events) == 0 && !queueActive(events)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(session.Worktree.Path, "should-not-exist.txt")); !os.IsNotExist(err) {
		t.Fatalf("interrupted queued turn ran: stat error = %v", err)
	}
	assertTurnIdentities(t, live.events.after(firstBefore), 1)

	proof := "durable-acp-recovered-" + string(backend)
	before := live.events.len()
	if _, err := live.send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt: "Use a tool to create durable-acp-recovered.txt containing exactly " + proof +
			" and nothing else. Do not modify another file. Reply exactly done.",
	}); err != nil {
		t.Fatalf("send recovery turn: %v", err)
	}
	assertTurnSucceeded(t, ctx, live.events, before)
	assertTurnIdentities(t, live.events.after(before), 1)
	assertFileText(t, filepath.Join(session.Worktree.Path, "durable-acp-recovered.txt"), proof)
	closeAndRemove(t, ctx, live.engineValue(t), session)
}

// runPermissionRoundTrip deliberately uses the provider's guarded mode. A
// real ACP permission callback must be surfaced by Engine and the host's
// approval must let the model complete its file edit.
func runPermissionRoundTrip(t *testing.T, backend host.Backend) {
	t.Helper()
	ctx, cancel := realContext(t)
	defer cancel()
	live := newLiveEngine(t, ctx, backend)
	proof := "durable-acp-permission-" + string(backend)
	before := live.events.len()
	session := live.start(t, ctx, durableacp.StartRequest{
		ID:             "permission-round-trip",
		Backend:        backend,
		WorkspaceMode:  durableacp.WorkspaceManaged,
		Source:         newGitRepository(t),
		Model:          modelFor(backend),
		Reasoning:      reasoningFor(backend),
		PermissionMode: permissionModeFor(backend),
		Prompt: "Use a shell or file-editing tool to create durable-acp-permission.txt containing " +
			"exactly " + proof + " and nothing else. Do not modify another file. Reply exactly done.",
	})
	assertTurnSucceeded(t, ctx, live.events, before)
	after := live.events.after(before)
	assertTurnIdentities(t, after, 1)
	if !hasPermissionInteraction(after) {
		t.Fatalf("%s completed guarded edit without a standard ACP permission interaction; events: %s", backend, eventSummary(after))
	}
	if !hasInteractionResolution(after) {
		t.Fatalf("%s permission was not resolved through Engine.RespondInteraction; events: %s", backend, eventSummary(after))
	}
	assertFileText(t, filepath.Join(session.Worktree.Path, "durable-acp-permission.txt"), proof)
	closeAndRemove(t, ctx, live.engineValue(t), session)
}

// runNativePlanMode covers providers which expose a session plan mode but do
// not implement ACP's request-permission callback. It verifies the mode is
// configured before a live turn and that the real agent cannot mutate files.
func runNativePlanMode(t *testing.T, backend host.Backend) {
	t.Helper()
	ctx, cancel := realContext(t)
	defer cancel()
	live := newLiveEngine(t, ctx, backend)
	before := live.events.len()
	session := live.start(t, ctx, durableacp.StartRequest{
		ID:             "native-plan-mode",
		Backend:        backend,
		WorkspaceMode:  durableacp.WorkspaceManaged,
		Source:         newGitRepository(t),
		Model:          modelFor(backend),
		Reasoning:      reasoningFor(backend),
		PermissionMode: "plan",
		Prompt: "Use a tool to create durable-acp-plan-mode-forbidden.txt containing exactly forbidden. " +
			"Do not modify any other file. Reply with a short plan explaining why this cannot be edited in plan mode.",
	})
	assertTurnSucceeded(t, ctx, live.events, before)
	assertTurnIdentities(t, live.events.after(before), 1)
	if _, err := os.Stat(filepath.Join(session.Worktree.Path, "durable-acp-plan-mode-forbidden.txt")); !os.IsNotExist(err) {
		t.Fatalf("%s plan mode modified a file: stat error = %v", backend, err)
	}
	if hasPermissionInteraction(live.events.after(before)) {
		t.Fatalf("%s emitted a standard ACP permission interaction despite testing native plan mode", backend)
	}
	closeAndRemove(t, ctx, live.engineValue(t), session)
}

type sendOutcome struct {
	result runtime.SendResult
	err    error
}

type liveProvider struct {
	backend    host.Backend
	newAdapter func() host.Adapter
}

func requireProvider(t *testing.T, backend host.Backend) liveProvider {
	t.Helper()
	key := "DURABLE_ACP_REAL_" + strings.ToUpper(string(backend)) + "_ACP"
	command := strings.TrimSpace(os.Getenv(key))
	if command == "" {
		t.Skipf("set %s or use scripts/run-realacp.sh", key)
	}
	if _, err := exec.LookPath(command); err != nil && !filepath.IsAbs(command) {
		t.Fatalf("resolve %s=%q: %v", key, command, err)
	}
	args := strings.Fields(os.Getenv(key + "_ARGS"))
	return liveProvider{
		backend: backend,
		newAdapter: func() host.Adapter {
			options := []acpx.Option{
				acpx.WithCommand(command),
				acpx.WithArgs(args...),
				acpx.WithStderr(os.Stderr),
			}
			switch backend {
			case claude.Backend:
				return claude.New(options...)
			case codex.Backend:
				return codex.New(options...)
			case cursor.Backend:
				return cursor.New(options...)
			case antigravity.Backend:
				return antigravity.New(options...)
			default:
				t.Fatalf("unsupported live backend %q", backend)
				return nil
			}
		},
	}
}

type liveEngine struct {
	t        *testing.T
	ctx      context.Context
	provider liveProvider
	events   *eventRecorder

	mu     sync.RWMutex
	engine *durableacp.Engine
	home   string
}

func newLiveEngine(t *testing.T, ctx context.Context, backend host.Backend) *liveEngine {
	t.Helper()
	live := &liveEngine{t: t, ctx: ctx, provider: requireProvider(t, backend), events: newEventRecorder(), home: filepath.Join(t.TempDir(), "state")}
	live.open(t, ctx)
	t.Cleanup(func() { live.close() })
	return live
}

func (l *liveEngine) open(t *testing.T, ctx context.Context) {
	t.Helper()
	opened, err := durableacp.Open(ctx, l.home,
		durableacp.WithAdapters(l.provider.newAdapter()),
		durableacp.WithEventSink(l.onEvent),
	)
	if err != nil {
		t.Fatalf("open live engine: %v", err)
	}
	l.mu.Lock()
	l.engine = opened
	l.mu.Unlock()
}

func (l *liveEngine) reopen(t *testing.T, ctx context.Context) {
	t.Helper()
	l.close()
	l.open(t, ctx)
}

func (l *liveEngine) close() {
	l.mu.Lock()
	engine := l.engine
	l.engine = nil
	l.mu.Unlock()
	if engine != nil {
		_ = engine.Close()
	}
}

func (l *liveEngine) engineValue(t *testing.T) *durableacp.Engine {
	t.Helper()
	l.mu.RLock()
	engine := l.engine
	l.mu.RUnlock()
	if engine == nil {
		t.Fatal("live engine is closed")
	}
	return engine
}

func (l *liveEngine) start(t *testing.T, ctx context.Context, request durableacp.StartRequest) durableacp.Session {
	t.Helper()
	session, err := l.engineValue(t).Start(ctx, request)
	if err != nil {
		t.Fatalf("start %s session: %v", l.provider.backend, err)
	}
	return session
}

func (l *liveEngine) send(ctx context.Context, request host.SendTurnRequest) (runtime.SendResult, error) {
	l.mu.RLock()
	engine := l.engine
	l.mu.RUnlock()
	if engine == nil {
		return runtime.SendResult{}, fmt.Errorf("live engine is closed")
	}
	return engine.Send(ctx, request)
}

func (l *liveEngine) onEvent(event host.Event) {
	l.events.append(event)
	if event.Type != host.EventInteractionRequested || event.Interaction == nil {
		return
	}
	response := host.InteractionResponse{RequestID: event.Interaction.ID, Action: "cancel"}
	switch event.Interaction.Kind {
	case host.InteractionPermission:
		response.Action = "approve"
	case host.InteractionChoice, host.InteractionForm, host.InteractionPlan:
		response.Action = "submit"
	}
	go func(sessionID string, response host.InteractionResponse) {
		l.mu.RLock()
		engine := l.engine
		l.mu.RUnlock()
		if engine == nil {
			return
		}
		if err := engine.RespondInteraction(l.ctx, sessionID, response); err != nil {
			l.events.appendError(fmt.Errorf("respond %s interaction: %w", event.Interaction.Kind, err))
		}
	}(event.SessionID, response)
}

func assertDiscovery(t *testing.T, ctx context.Context, live *liveEngine, backend host.Backend) {
	t.Helper()
	engine := live.engineValue(t)
	found := false
	for _, status := range engine.Runtime().Detect(ctx) {
		if status.Backend != backend {
			continue
		}
		if !status.Available {
			t.Fatalf("backend %q unavailable: %s", backend, status.Error)
		}
		found = true
	}
	if !found {
		t.Fatalf("backend %q was not registered", backend)
	}
	catalog, ok := engine.Runtime().Catalog(ctx, true)[backend]
	if !ok {
		t.Fatalf("catalog did not retain configured backend %q", backend)
	}
	if len(catalog.Models) == 0 && len(catalog.PermissionModes) == 0 && len(catalog.Reasoning) == 0 && len(catalog.SlashCommands) == 0 {
		t.Fatalf("catalog for backend %q is empty", backend)
	}
	if _, err := os.Stat(filepath.Join(live.home, "cache", "model-catalog.json")); err != nil {
		t.Fatalf("catalog cache missing after discovery: %v", err)
	}
}

func realContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	if testing.Short() {
		t.Skip("real ACP tests are disabled by -short")
	}
	return context.WithTimeout(context.Background(), realTestTimeout)
}

func modelFor(backend host.Backend) string {
	key := "DURABLE_ACP_REAL_" + strings.ToUpper(string(backend)) + "_MODEL"
	if model := strings.TrimSpace(os.Getenv(key)); model != "" {
		return model
	}
	return strings.TrimSpace(os.Getenv(realModelEnv))
}

func reasoningFor(backend host.Backend) string {
	key := "DURABLE_ACP_REAL_" + strings.ToUpper(string(backend)) + "_REASONING"
	if reasoning := strings.TrimSpace(os.Getenv(key)); reasoning != "" {
		return reasoning
	}
	return strings.TrimSpace(os.Getenv(realReasoningEnv))
}

func permissionModeFor(backend host.Backend) string {
	key := "DURABLE_ACP_REAL_" + strings.ToUpper(string(backend)) + "_PERMISSION_MODE"
	if mode := strings.TrimSpace(os.Getenv(key)); mode != "" {
		return mode
	}
	if mode := strings.TrimSpace(os.Getenv(realPermissionModeEnv)); mode != "" {
		return mode
	}
	// Codex's normal agent mode permits workspace edits, so read-only is needed
	// to force a real ACP request-permission round trip. Claude's default mode
	// already asks for tool permission; its plan mode deliberately executes no
	// tools and therefore cannot exercise the permission callback.
	if backend == codex.Backend {
		return "read-only"
	}
	return ""
}

func writablePermissionModeFor(backend host.Backend) string {
	key := "DURABLE_ACP_REAL_" + strings.ToUpper(string(backend)) + "_WRITABLE_PERMISSION_MODE"
	if mode := strings.TrimSpace(os.Getenv(key)); mode != "" {
		return mode
	}
	// agy runs headlessly in this suite. Its native permission prompts cannot
	// be surfaced through ACP yet, so it must be explicitly configured to edit.
	if backend == antigravity.Backend {
		return "bypassPermissions"
	}
	return ""
}

func newGitRepository(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	for _, args := range [][]string{
		{"init", "--initial-branch=main"},
		{"config", "user.email", "realacp@example.invalid"},
		{"config", "user.name", "durable-acp realacp"},
	} {
		runGit(t, directory, args...)
	}
	if err := os.WriteFile(filepath.Join(directory, "README.md"), []byte("# real ACP fixture\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	runGit(t, directory, "add", "README.md")
	runGit(t, directory, "commit", "-m", "real ACP fixture")
	return directory
}

func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func closeAndRemove(t *testing.T, ctx context.Context, engine *durableacp.Engine, session durableacp.Session) {
	t.Helper()
	if err := engine.CloseSession(session.ID); err != nil {
		t.Fatalf("close session %q: %v", session.ID, err)
	}
	force := session.WorkspaceMode == durableacp.WorkspaceManaged
	if err := engine.Remove(ctx, session.ID, force); err != nil {
		t.Fatalf("remove session %q: %v", session.ID, err)
	}
}

func assertSameDirectory(t *testing.T, actual, expected string) {
	t.Helper()
	actualInfo, err := os.Stat(actual)
	if err != nil {
		t.Fatalf("stat actual workspace %q: %v", actual, err)
	}
	expectedInfo, err := os.Stat(expected)
	if err != nil {
		t.Fatalf("stat expected workspace %q: %v", expected, err)
	}
	if !os.SameFile(actualInfo, expectedInfo) {
		t.Fatalf("existing workspace path = %q, want %q", actual, expected)
	}
}

func assertManagedWorktree(t *testing.T, session durableacp.Session) {
	t.Helper()
	if session.Worktree.Path == "" || session.Worktree.Branch == "" {
		t.Fatalf("managed worktree is incomplete: %+v", session.Worktree)
	}
	if !strings.HasPrefix(session.Worktree.Branch, "durable-acp/") {
		t.Fatalf("managed branch = %q, want durable-acp prefix", session.Worktree.Branch)
	}
}

func assertSessionListed(t *testing.T, engine *durableacp.Engine, id string, closed bool) {
	t.Helper()
	sessions, err := engine.Sessions()
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	for _, session := range sessions {
		if session.ID == id {
			if (session.ClosedAt != nil) != closed {
				t.Fatalf("session %q closed = %t, want %t", id, session.ClosedAt != nil, closed)
			}
			return
		}
	}
	t.Fatalf("session %q missing from %+v", id, sessions)
}

func assertSessionAbsent(t *testing.T, engine *durableacp.Engine, id string) {
	t.Helper()
	sessions, err := engine.Sessions()
	if err != nil {
		t.Fatalf("list sessions after removal: %v", err)
	}
	for _, session := range sessions {
		if session.ID == id {
			t.Fatalf("removed session still listed: %+v", session)
		}
	}
}

func assertFileText(t *testing.T, path, want string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		entries, _ := os.ReadDir(filepath.Dir(path))
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("read %s: %v (workspace entries: %s)", path, err, strings.Join(names, ","))
	}
	if got := strings.TrimSpace(string(content)); got != want {
		t.Fatalf("file %s = %q, want %q", path, got, want)
	}
}

func assertJournal(t *testing.T, records []journal.Record, names ...string) {
	t.Helper()
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		seen[record.Event] = true
	}
	for _, name := range names {
		if !seen[name] {
			t.Fatalf("journal events = %#v, missing %q", seen, name)
		}
	}
}

func assertLiveEditingStream(t *testing.T, events []host.Event) {
	t.Helper()
	if !hasEvent(events, host.EventMessage) {
		t.Fatalf("live coding stream omitted the agent response: %s", eventSummary(events))
	}
}

func assertTurnSucceeded(t *testing.T, ctx context.Context, recorder *eventRecorder, start int) {
	t.Helper()
	if err := recorder.wait(ctx, "successful live turn", func(events []host.Event) bool {
		for _, event := range events[start:] {
			if event.Type == host.EventTurnFailed {
				return false
			}
			if event.Type == host.EventTurnComplete {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatal(err)
	}
	for _, event := range recorder.after(start) {
		if event.Type == host.EventTurnFailed {
			t.Fatalf("live turn failed: %s", event.Message)
		}
	}
	if errors := recorder.errors(); len(errors) > 0 {
		t.Fatalf("interaction response errors: %v", errors)
	}
}

func assertNoTurnFailure(t *testing.T, events []host.Event) {
	t.Helper()
	for _, event := range events {
		if event.Type == host.EventTurnFailed {
			t.Fatalf("live turn failed: %s", event.Message)
		}
	}
}

func assertTurnIdentities(t *testing.T, events []host.Event, expected int) {
	t.Helper()
	active := ""
	started := 0
	for _, event := range events {
		switch event.Type {
		case host.EventTurnStarted:
			if event.BackendTurnID == "" {
				t.Fatalf("turn_started omitted identity: %s", eventSummary(events))
			}
			active = event.BackendTurnID
			started++
		case host.EventMessage, host.EventThinking, host.EventToolStarted, host.EventToolOutput, host.EventPlanUpdate, host.EventInteractionRequested:
			if active != "" && event.BackendTurnID != active {
				t.Fatalf("turn event %q identity = %q, want %q: %s", event.Type, event.BackendTurnID, active, eventSummary(events))
			}
		case host.EventTurnComplete, host.EventTurnFailed:
			if active != "" && event.BackendTurnID != active {
				t.Fatalf("terminal identity = %q, want %q: %s", event.BackendTurnID, active, eventSummary(events))
			}
			active = ""
		default:
		}
	}
	if started != expected {
		t.Fatalf("turn starts = %d, want %d: %s", started, expected, eventSummary(events))
	}
}

func assertQueueTransition(t *testing.T, events []host.Event, want ...int) {
	t.Helper()
	seen := make([]int, 0, len(events))
	for _, event := range events {
		if event.Type != host.EventQueueUpdated {
			continue
		}
		depth, ok := event.Data["queue_depth"].(int)
		if ok {
			seen = append(seen, depth)
		}
	}
	for _, expected := range want {
		found := false
		for _, depth := range seen {
			if depth == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("queue depths = %v, missing %d", seen, expected)
		}
	}
}

func hasPermissionInteraction(events []host.Event) bool {
	for _, event := range events {
		if event.Type == host.EventInteractionRequested && event.Interaction != nil && event.Interaction.Kind == host.InteractionPermission {
			return true
		}
	}
	return false
}

func hasInteractionResolution(events []host.Event) bool {
	for _, event := range events {
		if event.Type == host.EventInteractionResolved && event.InteractionResponse != nil && event.InteractionResponse.Action == "approve" {
			return true
		}
	}
	return false
}

func hasEvent(events []host.Event, kind host.EventType) bool {
	return countEvents(events, kind) > 0
}

func countEvents(events []host.Event, kind host.EventType) int {
	count := 0
	for _, event := range events {
		if event.Type == kind {
			count++
		}
	}
	return count
}

func queueDepth(events []host.Event) int {
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.Type != host.EventQueueUpdated {
			continue
		}
		if depth, ok := event.Data["queue_depth"].(int); ok {
			return depth
		}
	}
	return -1
}

func queueActive(events []host.Event) bool {
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.Type != host.EventQueueUpdated {
			continue
		}
		active, _ := event.Data["active"].(bool)
		return active
	}
	return true
}

func eventSummary(events []host.Event) string {
	types := make([]string, 0, len(events))
	for _, event := range events {
		label := string(event.Type)
		if message := strings.TrimSpace(event.Message); message != "" {
			label += "=" + message
		}
		types = append(types, label)
	}
	return strings.Join(types, ",")
}

type eventRecorder struct {
	mu      sync.Mutex
	events  []host.Event
	errs    []error
	changed chan struct{}
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{changed: make(chan struct{})}
}

func (r *eventRecorder) append(event host.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.signalLocked()
	r.mu.Unlock()
}

func (r *eventRecorder) appendError(err error) {
	r.mu.Lock()
	r.errs = append(r.errs, err)
	r.signalLocked()
	r.mu.Unlock()
}

func (r *eventRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func (r *eventRecorder) snapshot() []host.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]host.Event(nil), r.events...)
}

func (r *eventRecorder) after(start int) []host.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if start < 0 {
		start = 0
	}
	if start > len(r.events) {
		start = len(r.events)
	}
	return append([]host.Event(nil), r.events[start:]...)
}

func (r *eventRecorder) count(kind host.EventType) int {
	return countEvents(r.snapshot(), kind)
}

func (r *eventRecorder) errors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.errs...)
}

func (r *eventRecorder) wait(ctx context.Context, description string, condition func([]host.Event) bool) error {
	for {
		r.mu.Lock()
		snapshot := append([]host.Event(nil), r.events...)
		changed := r.changed
		r.mu.Unlock()
		if condition(snapshot) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s: %w; events: %s", description, ctx.Err(), eventSummary(snapshot))
		case <-changed:
		}
	}
}

func (r *eventRecorder) signalLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}
