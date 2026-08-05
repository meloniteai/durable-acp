// Package cursor provides the bundled Cursor ACP adapter.
package cursor

import (
	"time"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

// Backend is the stable backend name used in Engine session manifests.
const Backend host.Backend = "cursor"

const defaultCommand = "cursor-agent"

// Adapter drives the Cursor command-line agent in ACP mode.
type Adapter struct{ *acpx.Adapter }

// New creates an adapter for Cursor's conventional `cursor-agent acp` command. Use
// acpx.WithCommand or acpx.WithArgs for a different installation.
func New(options ...acpx.Option) *Adapter {
	capabilities := acp.ClientCapabilities{Meta: map[string]any{"parameterizedModelPicker": true}}
	return &Adapter{Adapter: acpx.New(acpx.Config{
		Backend:                 Backend,
		Command:                 defaultCommand,
		Args:                    []string{"acp"},
		ClientCapabilities:      &capabilities,
		RestartOnExit:           true,
		LegacyExtensions:        true,
		BestEffortConfiguration: true,
		SessionModeValues:       []string{"agent", "plan", "ask"},
		DoneCompletionGrace:     325 * time.Millisecond,
		CompleteOnDone:          true,
		ModelInPrompt:           true,
	}, options...)}
}
