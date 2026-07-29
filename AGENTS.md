# durable-acp

Keep this repository provider- and product-neutral.

- `acp`: generated ACP wire types and method metadata; no process or network I/O.
- `transport`: JSON-RPC process transport.
- `client`: initialized ACP client connections built on `transport`.
- `host`: normalized adapter contracts and opaque extension seams.
- `session`: universal session state transitions.
- `conformance`: reusable protocol and adapter contract checks.

Dependencies point inward: orchestration accepts `host.Adapter`; composition code imports concrete adapters. Put product data in `Ext`, `Data`, or process-local metadata—never shared fields. Avoid product vocabulary and dependencies, prefer the standard library, and keep packages independently testable.

Before merging, run `make all` and inspect the import graph and targeted product-leak checks.
