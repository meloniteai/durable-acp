# durable-acp

`durable-acp` is a standalone Go toolkit for building durable clients and hosts for the [Agent Client Protocol](https://agentclientprotocol.com/). It launches ACP agents over standard I/O and implements the complete stable ACP v1 client-side method surface.

## What is included

- Complete stable ACP v1 wire types, generated from the official schema snapshot identified by `acp.SchemaRevision`.
- Every client-to-agent method: initialization, authentication, session creation/loading/listing/deletion/resumption/closing, modes, configuration, prompting, and cancellation.
- Every agent-to-client callback: session updates, permissions, file reads and writes, terminal lifecycle, and elicitation.
- Bidirectional JSON-RPC 2.0 over newline-delimited standard I/O, including concurrent requests, typed errors, request cancellation, and extension methods.
- A provider-neutral `host.Adapter` seam and a small validated `session` lifecycle model for higher-level runtimes.

This repository implements the ACP client/host role. It is not an agent implementation, a worktree manager, or a bundle of vendor-specific adapters.

## Install

```sh
go get github.com/meloniteai/durable-acp
```

The module currently requires Go 1.26.

## Quick start

The handler may implement only the callback capabilities the host supports. This example streams session updates and leaves filesystem, terminal, elicitation, and permission callbacks disabled.

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
| `session` | Optional host-side session identity and validated lifecycle |
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
