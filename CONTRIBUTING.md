# Contributing

Issues and pull requests are welcome. Please keep changes focused and include tests for behavior changes.

Before submitting a change, run:

```sh
make lint
make vet
make test
```

Use `gofmt` for Go source and conventional commit subjects where practical.

## Updating ACP v1

Do not edit `acp/schema_*_gen.go` directly.

1. Update the official schema revision and the two expected SHA-256 digests in `internal/cmd/acpgen/main.go`.
2. Run `go generate ./acp`.
3. Update `acp.SchemaRevision` and, if needed, the method matrix in `acp/methods.go`.
4. Update the conformance assertions and add wire-level regression tests for changed types or behavior.
5. Run the complete validation suite.

Only the official stable `schema/v1/schema.json` and `schema/v1/meta.json` are in scope. Draft-v2 and unstable schema additions must not leak into the stable `acp` package.

## Compatibility

Public API changes should preserve ACP wire compatibility and include a migration note. Provider-specific behavior belongs outside the core module or behind the provider-neutral interfaces in `host`.
