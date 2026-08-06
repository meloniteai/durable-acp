# durable-acp

`durable-acp` is a small, batteries-included Go backend for hosting
[Agent Client Protocol](https://agentclientprotocol.com/) agents. Embed one
`Engine` in an application and it manages provider processes, durable sessions,
bounded turn queues, normalized events, journals, snapshots, and optional Git
worktrees.

The library is deliberately product-neutral. It does not provide an application
RPC server, UI framework, authentication client, workflow engine, rule engine,
review system, or generic capability-discovery framework. Those concerns belong
to the embedding application.

## What you get

- A concrete `durableacp.Engine` for opening, starting, resuming, controlling,
  inspecting, closing, and removing agent sessions.
- Bundled adapters for Claude, Codex, Cursor, and Antigravity, plus a reusable
  standard ACP subprocess adapter.
- Provider-neutral events and interactions suitable for a desktop app, TUI, or
  service.
- Serialized turns with a bounded, editable queue per session.
- Append-only JSONL history, durable sequence cursors, snapshots, and replay
  deduplication on provider resume.
- Managed Git worktrees or safe use of a host-owned workspace.
- Complete stable ACP v1 wire types and a lower-level typed ACP client when the
  Engine is not the right abstraction.

The root `durableacp` package is the recommended API. Packages such as
`runtime`, `journal`, `worktree`, and `client` remain available for hosts
that need a lower layer.

## Install

```sh
go get github.com/meloniteai/durable-acp
```

The module requires Go 1.26.

Provider executables are separate installations. Opening an Engine does not
download software, authenticate a user, or start a provider process.

## Quick start

`Open` takes an absolute application-owned state directory. It never discovers
a home from an environment variable or command-line flag.

```go
package main

import (
	"context"
	"log"

	durableacp "github.com/meloniteai/durable-acp"
	"github.com/meloniteai/durable-acp/host"
)

func main() {
	ctx := context.Background()
	engine, err := durableacp.Open(
		ctx,
		"/absolute/path/to/app-state",
		durableacp.WithEventSink(func(event host.Event) {
			log.Printf("%s %s", event.Type, event.Message)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := engine.Close(); err != nil {
			log.Printf("close durable-acp: %v", err)
		}
	}()

	session, err := engine.Start(ctx, durableacp.StartRequest{
		Backend: "codex",
		Source:  "/absolute/path/to/git-repository",
		Prompt:  "Explain this repository and suggest the first change.",
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := engine.Send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt:    "Implement the change and run the relevant tests.",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("accepted=%t queued=%t", result.Accepted, result.Queued)
}
```

The default workspace mode is managed: `Start` creates a worktree and an owned
branch from the local Git repository in `Source`. The initial prompt starts
immediately. A later `Send` starts immediately when the session is idle or
enters its queue when a turn is active.

## Engine at a glance

| Concern | Engine methods |
| --- | --- |
| Providers | `Detect`, `Catalog`, `RefreshCatalog`, `SetCatalog` |
| Sessions | `Start`, `Resume`, `Sessions`, `Snapshot` |
| Turns | `Send`, `SendNext`, `Interrupt`, `InterruptActive`, `Restart` |
| Queue | `QueueEntries`, `ReplaceQueuedTurn`, `RemoveQueuedTurn`, `BlockDispatch`, `UnblockDispatch` |
| Configuration | `SetConfiguration` |
| Interactions | `PendingInteraction`, `RespondInteraction`, `ForkPrompt` |
| History | `History`, `HistoryAfter`, `HistoryTail`, `Append`, `Journal` |
| Workspaces | `Repair`, `Prune` |
| Cleanup | `CloseSession`, `Remove`, `Close` |

`Snapshot` is the single provider-neutral read model for a live or persisted
session. It includes lifecycle state, workspace ownership, provider identifiers,
the active turn, the ordered queue, dispatch state, pending interaction,
effective configuration, the last journal sequence, and the session extension
payload.

## Sessions and workspaces

Each Engine session binds a durable session ID to one backend and one workspace.
The backend may expose its own session, thread, and turn identifiers; durable-acp
stores those as opaque references.

### Managed workspaces

Managed mode is the default:

```go
session, err := engine.Start(ctx, durableacp.StartRequest{
	Backend: "claude",
	Source:  "/absolute/path/to/git-repository",
})
```

The Engine creates a private worktree below its state directory and checks out a
new branch named with the persisted branch prefix. It owns that worktree and
branch and may remove them only through an explicit `Remove`.

Use `WithBranchPrefix("my-app")` to select a different managed branch prefix.
Use `Repair` after an interrupted process or manual Git operation and `Prune`
to remove stale Git worktree registrations from the source repository.

### Existing workspaces

Use an existing absolute directory when the host owns the workspace:

```go
session, err := engine.Start(ctx, durableacp.StartRequest{
	Backend:       "cursor",
	WorkspaceMode: durableacp.WorkspaceExisting,
	Worktree:      "/absolute/path/to/existing-workspace",
})
```

The Engine records Git metadata when available but does not create, switch,
relocate, or delete a host-owned workspace. `Repair` only reinspects it, and
`Remove` only removes the Engine manifest.

### Resume and cleanup

Calling `Engine.Close` stops provider processes and closes Engine-owned
resources. It leaves manifests, journals, branches, and worktrees intact.
Reopen the same state directory and resume an active session:

```go
engine, err := durableacp.Open(ctx, stateDir)
if err != nil {
	return err
}
if _, err := engine.Resume(ctx, sessionID); err != nil {
	return err
}
```

`Resume` reconnects with the last durable backend session ID and does not
create a replacement workspace.

Cleanup operations have intentionally different scopes:

| Operation | Result |
| --- | --- |
| `Engine.Close` | Stops all provider processes; active sessions remain resumable |
| `CloseSession` | Stops one provider and marks its session closed; keeps its manifest, workspace, and journal for inspection |
| `Remove` | Closes if needed, removes an owned worktree and branch, deletes the manifest, and forgets runtime state |

`Remove` never deletes a host-owned existing workspace. Session journals remain
available after removal. A session marked by `CloseSession` is no longer
resumable; use `Engine.Close` when the intent is process shutdown followed by
later recovery.

## Turns, queues, and control

Only one provider turn runs at a time in a session.

- `Send` dispatches immediately when idle and otherwise appends to the queue.
- `SendNext` dispatches immediately when idle and otherwise prepends to the
  queue.
- Queue entries have stable IDs and retain the complete opaque
  `host.SendTurnRequest`.
- `ReplaceQueuedTurn` changes an entry without changing its position.
- `RemoveQueuedTurn` removes only the requested entry.
- `BlockDispatch` lets the active turn finish while preventing the next queued
  turn from starting. `UnblockDispatch` resumes dispatch.

The default maximum is 16 queued turns per session. Configure it with
`WithRuntimeConfiguration(runtime.Config{MaxQueuedTurns: ...})`.

```go
entries, err := engine.QueueEntries(session.ID)
if err != nil {
	return err
}
for _, entry := range entries {
	log.Printf("%s: %s", entry.ID, entry.Request.Prompt)
}

if len(entries) > 0 {
	if _, err := engine.RemoveQueuedTurn(session.ID, entries[0].ID); err != nil {
		return err
	}
}
```

`Interrupt` cancels the active turn and clears queued turns.
`InterruptActive` cancels only the active turn and preserves queued work. If an
adapter successfully interrupts but emits no terminal event, the runtime emits a
neutral `turn_failed` event so the session cannot remain stuck as active.

`Restart` restarts only an idle provider process. It preserves the Engine
session, workspace, queue, configuration, and journal.

## Events and interactions

Subscribe at open time with `WithEventSink` or later with `Subscribe`.
`Subscribe` returns a function that removes that listener.

Adapters produce one provider-neutral `host.Event` stream. It covers messages,
thinking, tools, file changes, plans, configuration choices, queue changes,
turn lifecycle, process lifecycle, and user interactions. Turn-scoped events
carry the same backend turn ID from `turn_started` through
`turn_completed` or `turn_failed`.

The runtime coalesces adjacent streaming updates before publishing them.
Provider event identifiers are retained as `SourceEventID` when available so
hosts can keep stable projections.

Permission, choice, form, and plan requests are represented by
`host.InteractionRequest`:

```go
pending, err := engine.PendingInteraction(session.ID)
if err != nil {
	return err
}
if pending != nil {
	if err := engine.RespondInteraction(ctx, session.ID, host.InteractionResponse{
		RequestID: pending.ID,
		Action:    "approve",
		OptionID:  "allow-once",
	}); err != nil {
		return err
	}
}
```

The Engine does not render interactions or decide approval policy. The host
chooses how to present and answer them.

## History, journals, and replay

Each session has an append-only JSONL journal. Normalized runtime events are
translated to a small neutral vocabulary such as `user.message`,
`agent.message`, `agent.turn_started`, `agent.yielded`, and
`agent.interaction_requested`.

High-volume presentation events such as thinking, tool output, trace updates,
and queue updates are live-only by default. Product-specific history projections
belong in the host.

History methods use monotonically increasing durable sequence numbers:

```go
snapshot, err := engine.Snapshot(session.ID)
if err != nil {
	return err
}

records, err := engine.History(
	session.ID,
	lastSeenSequence,
	snapshot.LastJournalSequence,
)
if err != nil {
	return err
}
for _, record := range records {
	log.Printf("%d %s", record.Sequence, record.Event)
}
lastSeenSequence = snapshot.LastJournalSequence
```

- `HistoryAfter(id, sequence)` returns records after an exclusive cursor.
- `History(id, after, through)` adds an inclusive upper bound; zero leaves
  either side unbounded.
- `HistoryTail(id, limit)` returns recent records in ascending sequence order.
- `Journal()` exposes the raw store for advanced integrations.

When a bundled ACP adapter reloads provider history during `Resume`, the
runtime matches replayed semantic events against the durable journal and
suppresses duplicates. Unmatched events continue through the normal event and
journal paths.

Replay identity is an ordered occurrence coordinate: session, conversation,
backend thread, logical turn ordinal, event kind, and occurrence ordinal within
that turn. Logical turn ordinals are reconstructed independently for the
persisted and replay streams from user-message boundaries and provider turn
transitions. Provider event IDs and content are not identity inputs. Adjacent
cumulative snapshots with one source event ID are treated as one occurrence
inside their own stream. This keeps repeated identical messages distinct and
allows live and replay representations to use different IDs or payload
formatting.

Previously, two non-empty but different provider event IDs were a hard
mismatch, and recording that first miss advanced the replay cursor past the
remaining journal. Providers that mint new IDs for history replay therefore
duplicated the first event and every event after it. The matcher now compares
reconstructable occurrence coordinates, and unmatched replay records leave the
remaining journal matchable. Provider adapters can restore synthetic turn
counters from replay-local item identities without treating those item IDs as
durable canonical identity.

Hosts with an existing journal can pass a caller-owned store with
`WithJournalStore`, or configure an Engine-owned directory with
`WithJournalConfiguration`. Set `DisableRuntimeJournal` only when the host
already writes normalized runtime events into that same store.

## Opaque host extensions

Product data can cross the generic runtime without becoming part of its
vocabulary:

- `StartRequest.Ext`, `SendTurnRequest.Ext`, and `Session.Ext` carry opaque
  JSON.
- Adapter events may retain provider or host data in `host.Event.Data`.
- `Append` adds an application-owned journal record.

Extension journal names must be owner-qualified so they cannot collide with core
events:

```go
if _, err := engine.Append(
	session.ID,
	"example.review_requested",
	map[string]any{"review_id": "r-42"},
	nil,
); err != nil {
	return err
}
```

The Engine stores these payloads but does not interpret them.

## Unsupported operations and simple fallbacks

Some adapter operations are optional. An unsupported call returns an error that
matches `durableacp.ErrUnsupportedOperation`; the concrete
`UnsupportedOperationError` includes the backend and operation.

```go
_, err := engine.Restart(ctx, session.ID)
if errors.Is(err, durableacp.ErrUnsupportedOperation) {
	log.Printf("restart is unavailable for %s", session.Backend)
}
```

| Operation | Simple host fallback |
| --- | --- |
| `Restart` | Keep using the current provider session, or close the Engine and later `Resume` it |
| `ForkPrompt` | `Send` to the current session or explicitly `Start` another Engine session |
| `RespondInteraction` | Leave the request pending or call `InterruptActive`; never synthesize approval |
| Catalog discovery | Keep static defaults; `RefreshCatalog` reports the backend as `skip` |

This error-first contract is intentional. A host does not need to negotiate a
generic capability document before trying a focused optional operation.
`Detect` and catalog methods provide only provider availability and
configuration data.

## Native providers

`Open` includes four conventional adapters. None launch until a session starts
or a catalog refresh probes them.

| Backend | Conventional command |
| --- | --- |
| `claude` | `claude-agent-acp` |
| `codex` | `codex-acp` |
| `cursor` | `cursor-agent acp` |
| `antigravity` | `agy` |

`Detect` reports whether each command is available without launching a
provider. durable-acp does not install or update those commands and does not
manage their credentials.

Replace any bundled adapter by registering the same backend name:

```go
import (
	durableacp "github.com/meloniteai/durable-acp"
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/adapters/codex"
)

engine, err := durableacp.Open(
	ctx,
	stateDir,
	durableacp.WithAdapters(
		codex.New(acpx.WithCommand("/opt/agents/codex-acp")),
	),
)
```

`adapters/acpx` also supports custom arguments, a complete child environment,
stderr routing, client identity, and an entirely custom standard ACP executable.
Implement `host.Adapter` to integrate a provider that does not fit that
subprocess adapter.

## State layout

`Open` creates private directories and files below the supplied state
directory:

| Path | Purpose |
| --- | --- |
| `config.json` | Persisted Engine settings |
| `sessions/<id>.json` | Small durable session manifests |
| `journals/<id>.jsonl` | Append-only neutral history |
| `logs/providers/*.log` | Provider stderr diagnostics |
| `worktrees/<repository>/<session>/` | Engine-owned managed worktrees |
| `cache/model-catalog.json` | Best-effort provider catalog cache |

Directories use mode `0700` and private files use mode `0600`. A custom
journal directory replaces the default `journals` location rather than
duplicating it.

## Architectural boundary

durable-acp owns generic mechanics:

- ACP process transport and provider adapters
- session manifests and provider references
- serialized turns and queues
- normalized events and interactions
- journals, snapshots, replay matching, and catalog caching
- optional managed worktrees

The embedding product owns policy and presentation:

- RPC, HTTP, stdio, or application command boundaries
- authentication and account management
- UI state and product history projections
- orchestration, prompt composition, loops, and workflows
- rules, permissions policy, proofs, reviews, and approvals
- control-plane integration and operational modes

This separation keeps the Engine useful as a backend without turning it into an
application framework.

## Packages

| Package | Purpose |
| --- | --- |
| `durableacp` | Batteries-included Engine facade |
| `host` | Provider-neutral adapters, events, interactions, and requests |
| `runtime` | In-memory session multiplexer, queues, event routing, and catalog cache |
| `journal` | Append-only JSONL records, neutral translation, and replay matching |
| `worktree` | Managed Git worktree lifecycle |
| `adapters` | Bundled provider composition |
| `adapters/acpx` | Reusable standard ACP subprocess adapter |
| `acp` | Generated official stable-v1 wire schema and method metadata |
| `client` | Typed initialized ACP client connections and callbacks |
| `transport` | Concurrent bidirectional JSON-RPC process transport |
| `session` | Validated provider-neutral session lifecycle |
| `conformance` | Public ACP method-matrix and adapter assertions |

Use `client.Start` directly when building a custom ACP host that does not need
Engine-managed sessions, queues, journals, or worktrees. The wire-level `acp`
package preserves the standard; the `host` package is the intentionally
separate normalized model.

## Verification

The default suite is hermetic:

```sh
make all
```

This runs lint, vet, race-enabled tests, and an 85% minimum coverage check over
hand-written runtime code.

The opt-in real ACP suite drives the public Engine against actual providers and
models:

```sh
export OPENROUTER_API_KEY='...'
scripts/run-realacp.sh --provider openrouter
```

The runner covers managed and existing workspaces, queue serialization,
interrupt recovery, permission interactions, provider restart, Engine reopen
and resume, journal replay, repair, prune, and cleanup. It isolates each journey
with private Engine state, repositories, worktrees, provider homes, and logs.

Use `--agents` and `--journeys` to select subsets. Cursor and Antigravity
require their own normal credentials and cannot authenticate from an OpenRouter
key. The direct integration package is build-tagged:

```sh
go test -tags=realacp -v ./integration/realacp
```

## Schema provenance

Generated files in `acp/schema_*_gen.go` come from the official ACP stable-v1
`schema.json` and `meta.json` at `acp.SchemaRevision`. The generator pins
the source revision, generator version, and SHA-256 checksums.

```sh
go generate ./acp
git diff --exit-code -- acp/schema_*_gen.go
```

Generation verifies the pinned inputs and does not incorporate draft-v2 schema
changes.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and schema-update
guidance.
