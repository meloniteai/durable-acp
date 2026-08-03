# durable-acp

`durable-acp` is an embeddable, durable multi-agent runtime for the [Agent Client Protocol](https://agentclientprotocol.com/). Give it an absolute state directory and a repository; it manages sessions, journals, native ACP providers, turn queues, and optional Git worktrees. A desktop app or TUI only needs to render its events and call its SDK methods.

## What is included

- Complete stable ACP v1 wire types, generated from the official schema snapshot identified by `acp.SchemaRevision`.
- Every client-to-agent method: initialization, authentication, session creation/loading/listing/deletion/resumption/closing, modes, configuration, prompting, and cancellation.
- Every agent-to-client callback: session updates, permissions, file reads and writes, terminal lifecycle, and elicitation.
- Bidirectional JSON-RPC 2.0 over newline-delimited standard I/O, including concurrent requests, typed errors, request cancellation, and extension methods.
- A provider-neutral `host.Adapter` seam, normalized events/interactions, and a validated `session` lifecycle model.
- A batteries-included `durableacp.Engine` with native Claude, Codex, Cursor, and Antigravity ACP adapters.
- Durable JSONL journals, model-catalog caching, bounded turn queues, and full managed Git worktree lifecycle.

The recommended API is the root `durableacp` package. The lower-level ACP client remains available for hosts that need direct protocol control.

This release is intentionally an embedded SDK, not a generic stdio server or CLI. A host calls the Engine directly and is free to put a desktop UI, TUI, or its own RPC boundary above it.

## Install

```sh
go get github.com/meloniteai/durable-acp
```

The module currently requires Go 1.26.

## Recommended embedding

`Open` takes the absolute state directory as an SDK parameter. There is no SDK home environment variable, no `Config.Home`, and no CLI flag: the embedding application owns that decision. For example, an application that already owns `/Users/me/.my-app` passes that exact directory.

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
	engine, err := durableacp.Open(ctx, "/absolute/path/to/app-state")
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close() // stops child processes; sessions stay resumable

	engine.Subscribe(func(event host.Event) {
		// Render messages, tools, plans, queues, and interaction cards here.
		log.Printf("%s: %s", event.Type, event.Message)
	})

	session, err := engine.Start(ctx, durableacp.StartRequest{
		Backend: "codex", // claude, codex, cursor, or antigravity
		Source:  "/absolute/path/to/repository",
		Prompt:  "Explain this repository and suggest the first change.",
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = engine.Send(ctx, host.SendTurnRequest{
		SessionID: session.ID,
		Prompt:    "Implement the suggested change and run its tests.",
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

The first `Start` above creates a managed Git worktree and an owned branch. No Git state is removed on `Engine.Close`; reopening the same directory and calling `Resume(ctx, session.ID)` reconnects to the provider session. An application chooses when a session is truly done:

```go
// Stop the provider and retain its journal/worktree for later inspection.
if err := engine.CloseSession(session.ID); err != nil { /* handle */ }

// Explicit, destructive cleanup of a closed session's managed worktree,
// branch, and manifest. Its JSONL journal remains available for history.
if err := engine.Remove(ctx, session.ID, false); err != nil { /* handle */ }
```

For a host-managed workspace, set `WorkspaceMode: durableacp.WorkspaceExisting` and `Worktree` to an existing Git directory. Durable ACP inspects it but never deletes it. Managed worktrees can be checked and repaired after an interrupted process with `engine.Repair(ctx, sessionID)`; `engine.Prune(ctx, source)` only clears stale Git registrations.

### State layout

`Open` creates private (`0700` directories, `0600` files) state below the supplied directory:

| Path | Purpose |
| --- | --- |
| `config.json` | Durable ACP settings such as the managed branch prefix |
| `sessions/<id>.json` | Small session manifests used to reopen workspaces and providers |
| `journals/<id>.jsonl` | Append-only neutral session history |
| `logs/providers/*.log` | Native adapter stderr diagnostics |
| `logs/sessions/` | Reserved for host session diagnostics |
| `worktrees/<repository>/<session>/` | SDK-owned managed worktrees |
| `cache/model-catalog.json` | Best-effort provider model/mode catalog cache |

The default managed branch prefix is `durable-acp`; set `durableacp.WithBranchPrefix("my-agent")` when opening an engine to choose another prefix. The setting is persisted with the supplied home.

Lower-level `worktree.Manager.Create` derives its repository directory from the source remote by default. Hosts that already have a stable repository identifier can set `worktree.CreateRequest.RepositoryKey` to keep their existing layout; the value is sanitized and remains under the manager root.

For a workspace the host owns, `worktree.EnsureBranch(ctx, path, branch)` creates or checks out the requested branch without claiming, relocating, or deleting that workspace.

### Events, interactions, and application extensions

Every adapter emits the same `host.Event` vocabulary. A frontend can render `message`, `thinking`, tool, plan, queue, and `interaction_requested` events without knowing provider internals. Answer a permission, choice, form, or plan interaction with `engine.RespondInteraction`:

```go
err := engine.RespondInteraction(ctx, session.ID, host.InteractionResponse{
	RequestID: "interaction-7",
	Action:    "approve", // or deny, submit, decline, cancel
	OptionID:  "allow-once",
})
```

Applications can append their own opaque journal records without coupling to the runtime internals. Extension event names must be owner-qualified:

```go
_, err := engine.Append(session.ID, "example.review_requested", map[string]any{
	"review_id": "r-42",
}, nil)
```

The runtime is available for advanced UI startup work: `engine.Runtime().Detect(ctx)` reports native executable availability, and `engine.Runtime().Catalog(ctx, true)` refreshes provider models, modes, and reasoning choices. Most applications should otherwise use the Engine methods above.

### Native providers and customization

`Open` includes four conventional ACP commands without launching any of them until a session starts. `durable-acp` does not download, install, update, or bundle provider executables: each command must already be discoverable on `PATH` when the backend is detected or started.

| Backend | Default command |
| --- | --- |
| `claude` | `claude-agent-acp` |
| `codex` | `codex-acp` |
| `cursor` | `agent acp` |
| `antigravity` | `antigravity-acp` |

`engine.Runtime().Detect(ctx)` reports this prerequisite without starting a session. It returns an unavailable backend when the command is absent; it never installs software as a side effect. Some ACP adapters also require their provider CLI or credentials to be available according to that adapter's own documentation.

Install the provider-side command through its normal distribution channel before using the corresponding backend. If an application bundles or otherwise manages a sidecar, give durable-acp its absolute path; the other three remain PATH-based defaults:

```go
import (
	durableacp "github.com/meloniteai/durable-acp"
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/adapters/codex"
)

engine, err := durableacp.Open(ctx, stateDir,
	durableacp.WithAdapters(codex.New(acpx.WithCommand("/opt/agents/codex-acp"))),
)
```

The individual provider packages and `adapters/acpx` are public, so a host can configure arguments, a complete child environment, stderr, or an entirely custom ACP executable without importing runtime internals.

### Real ACP black-box coverage

The normal test suite is hermetic. The opt-in real ACP suite drives actual ACP
processes and actual coding models through the public `Engine`; it is not a
mocked protocol test. Each selected agent runs these isolated journeys:

| Journey | What it proves |
| --- | --- |
| `managed` | Detection, catalog caching, managed Git worktree/branch creation, live model edit, normalized stream events, live provider restart, neutral + opaque journals, repair/prune, Engine restart/provider resume, close, and owned cleanup. |
| `existing` | A caller-owned Git checkout survives `Repair` and `Remove`, while an initial `Start` turn receives an ACP text-resource attachment and uses it in a live edit. |
| `queued` | A second real model turn is queued while the first is active, then executes serially with the first turn's filesystem output available. |
| `interrupt` | A real shell tool call is cancelled, its queued follow-up is cleared, and the same provider session successfully completes a recovery turn. |
| `permission` | A guarded agent edit emits a standard ACP permission callback; `Engine.RespondInteraction` approves it and the model completes the edit. Antigravity instead verifies its advertised native plan mode because its bridge does not implement the standard callback. |

The runner installs and compiles once, then gives every agent/journey a private
`CODEX_HOME`, OS home, Engine state directory, repository, worktree, and log.
It runs those workers concurrently (four by default), so provider state cannot
leak between tests and slow model calls do not turn the suite into one serial
scenario. Set `DURABLE_ACP_REAL_JOBS` to tune concurrency and
`DURABLE_ACP_REAL_KEEP_ARTIFACTS=1` to retain a failed worker's evidence.

Run Codex and Claude Code through OpenRouter with the inexpensive low-effort
default model:

```sh
export OPENROUTER_API_KEY='...'
scripts/run-realacp.sh --provider openrouter
```

The runner installs pinned public Codex/Claude ACP adapters and coding CLIs
once into a temporary directory, compiles the Go test binary once, and runs all
five journeys for both agents. It defaults to `deepseek/deepseek-v4-flash`, low
reasoning, Codex's `read-only` mode for its permission journey, and Claude's
standard `default` permission mode. Override them with
`DURABLE_ACP_REAL_MODEL`, `DURABLE_ACP_REAL_REASONING`, and either
`DURABLE_ACP_REAL_PERMISSION_MODE` or an agent-specific variable such as
`DURABLE_ACP_REAL_CODEX_PERMISSION_MODE`. Run only a capability while debugging
with `--journeys interrupt`, or select several with a comma-separated list.

Cursor and Antigravity cannot be authenticated by an OpenRouter key. The runner
installs Cursor CLI into its temporary home when it is absent, but it still
needs normal Cursor authentication or `CURSOR_API_KEY`. For Antigravity it
builds the pinned [`antigravity-acp-go`](https://github.com/meloniteai/antigravity-acp-go)
bridge once and lets that bridge find or provision `agy`; normal `agy` OAuth or
provider credentials are still required. Ask the runner to include every native
agent explicitly:

```sh
# `agent acp` is the usual Cursor command.
export DURABLE_ACP_REAL_CURSOR_ACP="$(command -v agent)"
export DURABLE_ACP_REAL_CURSOR_ACP_ARGS='acp'

# Build the pinned bridge and run it with normal agy authentication.
scripts/run-realacp.sh --provider vanilla --agents antigravity

# Or override the bridge command while debugging a local bridge change.
export DURABLE_ACP_REAL_ANTIGRAVITY_ACP="$(command -v antigravity-acp)"
export DURABLE_ACP_REAL_ANTIGRAVITY_ACP_ARGS='--state-dir /tmp/agy-acp-real'
# _REPOSITORY and _REF override the source build when no command is supplied.

scripts/run-realacp.sh --provider vanilla --agents all --journeys all
```

The command variables accept absolute paths and corresponding `_ARGS` values
for nonstandard invocations. `--agents codex,claude,cursor,antigravity` and
`--journeys managed,existing,queued,interrupt,permission` select explicit
subsets. The direct Go tests are build-tagged and skip an unconfigured provider,
so use the runner for a complete setup:

```sh
go test -tags=realacp -v ./integration/realacp
```

GitHub Actions runs all five OpenRouter Codex-and-Claude journeys on relevant
pull requests and manual dispatches. Add `OPENROUTER_API_KEY` to
the repository Actions secrets; the workflow never exposes it and does not run
on fork pull requests, where secrets are unavailable. Cursor and Antigravity
need their own authenticated commands and credentials, so they cannot be made
truthfully green in that CI job from an OpenRouter secret alone; run their full
matrix in a credentialed environment with `--provider vanilla --agents all`.
The same workflow has a manual `native_agents` input (`cursor`, `antigravity`,
or `cursor,antigravity`): configure `DURABLE_ACP_CURSOR_API_KEY` and either
`DURABLE_ACP_ANTIGRAVITY_API_KEY` or `DURABLE_ACP_GEMINI_API_KEY` first.

## Low-level ACP connection

Use `client.Start` directly only when building a custom ACP host outside the durable Engine. The handler may implement only the callback capabilities the host supports. This example streams session updates and leaves filesystem, terminal, elicitation, and permission callbacks disabled.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/meloniteai/durable-acp/acp"
	acpclient "github.com/meloniteai/durable-acp/client"
)

type updates struct{}

func (updates) SessionUpdate(_ context.Context, notification *acp.SessionNotification) error {
	raw, err := json.Marshal(notification)
	if err != nil {
		return err
	}
	fmt.Println(string(raw))
	return nil
}

func main() {
	ctx := context.Background()
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	connection, err := acpclient.Start(ctx, acpclient.Spec{
		Command: "path-to-acp-agent",
		Handler: updates{},
		OnHandlerError: func(err error) {
			log.Printf("ACP callback: %v", err)
		},
		Initialize: acp.InitializeRequest{
			ClientInfo: &acp.Implementation{
				Name:    "example-host",
				Version: "dev",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()

	created, err := connection.NewSession(ctx, &acp.NewSessionRequest{
		Cwd:        cwd,
		McpServers: []acp.McpServer{},
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := connection.Prompt(ctx, &acp.PromptRequest{
		SessionId: created.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("Explain this repository.")},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stop reason:", result.StopReason)
}
```

Advertise callback support through `InitializeRequest.ClientCapabilities` and implement the corresponding interfaces:

- `SessionUpdateHandler`
- `PermissionHandler`
- `FileSystemHandler`
- `TerminalHandler`
- `ElicitationHandler`
- `ExtensionHandler`

Unsupported standard requests receive JSON-RPC `Method not found`. Unknown extension notifications are ignored as required by ACP.

## Packages

| Package | Purpose |
| --- | --- |
| `acp` | Official stable-v1 wire schema and method metadata; no I/O |
| `client` | Initialized typed ACP client connections, callbacks, capabilities, and extensions |
| `transport` | Concurrent bidirectional JSON-RPC process transport |
| `host` | Optional provider-neutral adapter and event model above the wire protocol |
| `journal` | Durable append-only JSONL session records and neutral host-event translation |
| `session` | Optional host-side session identity and validated lifecycle |
| `worktree` | Independent Git worktree create, reopen, repair, prune, and remove manager |
| `runtime` | Provider-neutral session registry, turn queue, event routing, and catalog cache |
| `adapters` | Native bundled provider composition |
| `adapters/acpx` | Reusable standard ACP subprocess adapter for custom providers |
| `conformance` | Assertions that the public method matrix matches the official v1 schema |

The wire-level `acp` package and normalized `host` package are intentionally separate. ACP types preserve the standard exactly; host types provide a convenient internal model for applications that normalize multiple backends.

## Schema provenance

Generated files in `acp/schema_*_gen.go` come from the official ACP v1 `schema.json` and `meta.json` at the commit in `acp.SchemaRevision`. The revision and both SHA-256 checksums are pinned in the generator.

To regenerate:

```sh
go generate ./acp
git diff --exit-code -- acp/schema_*_gen.go
```

Generation downloads the pinned schema and pinned Apache-2.0 Go generator, verifies the schema checksums, and never incorporates ACP's draft-v2 schema.

## Development

```sh
make lint
make vet
make test
```

`make test` runs the race detector and enforces at least 80% coverage over hand-written runtime code. Generated schema code and the generator command itself are excluded from the percentage, but are still compiled and exercised by conformance and integration tests.

See [CONTRIBUTING.md](CONTRIBUTING.md) for schema-update and contribution guidance.
