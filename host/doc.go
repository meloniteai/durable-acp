// Package host defines normalized events and adapter interfaces for ACP host runtimes.
//
// These types are deliberately separate from the wire-level ACP types in package
// acp. Adapters may use ACP or another provider protocol internally while exposing
// one provider-neutral stream to a host runtime.
package host
