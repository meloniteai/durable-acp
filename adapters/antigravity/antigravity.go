// Package antigravity provides the bundled Antigravity ACP adapter.
package antigravity

import (
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

// Backend is the stable backend name used in Engine session manifests.
const Backend host.Backend = "antigravity"

// Adapter drives an Antigravity-compatible ACP sidecar.
type Adapter struct{ *acpx.Adapter }

// New creates an adapter for the neutral antigravity-acp executable. Use
// acpx.WithCommand when a host distributes a differently named sidecar.
func New(options ...acpx.Option) *Adapter {
	return &Adapter{Adapter: acpx.New(acpx.Config{
		Backend: Backend,
		Command: "antigravity-acp",
	}, options...)}
}
