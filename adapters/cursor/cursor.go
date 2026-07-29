// Package cursor provides the bundled Cursor ACP adapter.
package cursor

import (
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

// Backend is the stable backend name used in Engine session manifests.
const Backend host.Backend = "cursor"

// Adapter drives the Cursor command-line agent in ACP mode.
type Adapter struct{ *acpx.Adapter }

// New creates an adapter for Cursor's conventional `agent acp` command. Use
// acpx.WithCommand or acpx.WithArgs for a different installation.
func New(options ...acpx.Option) *Adapter {
	return &Adapter{Adapter: acpx.New(acpx.Config{
		Backend: Backend,
		Command: "agent",
		Args:    []string{"acp"},
	}, options...)}
}
