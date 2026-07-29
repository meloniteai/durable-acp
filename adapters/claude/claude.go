// Package claude provides the bundled Claude ACP adapter.
package claude

import (
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

// Backend is the stable backend name used in Engine session manifests.
const Backend host.Backend = "claude"

// Adapter drives a Claude-compatible ACP sidecar.
type Adapter struct{ *acpx.Adapter }

// New creates an adapter for the standard claude-agent-acp executable. Use
// acpx.WithCommand when a host installs it elsewhere.
func New(options ...acpx.Option) *Adapter {
	return &Adapter{Adapter: acpx.New(acpx.Config{
		Backend: Backend,
		Command: "claude-agent-acp",
	}, options...)}
}
