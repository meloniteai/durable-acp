// Package acp defines the Agent Client Protocol version 1 wire contract.
//
// It exposes the complete stable ACP v1 schema and method metadata. Wire types
// are generated from a pinned snapshot of the official stable v1 schema. This
// package performs no process, network, or filesystem I/O.
//
//go:generate go run ../internal/cmd/acpgen -out .
package acp
