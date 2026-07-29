// Package adapters composes the ACP adapters bundled with durable-acp.
// Individual providers remain importable on their own when a host wants to
// customize a command or replace only one backend.
package adapters

import (
	"io"

	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/adapters/antigravity"
	"github.com/meloniteai/durable-acp/adapters/claude"
	"github.com/meloniteai/durable-acp/adapters/codex"
	"github.com/meloniteai/durable-acp/adapters/cursor"
	"github.com/meloniteai/durable-acp/host"
)

// Option customizes the complete bundled adapter set.
type Option func(*config)

type config struct {
	stderrFor func(host.Backend) io.Writer
}

// WithStderrFor directs each native adapter's stderr to a host-owned writer.
// Returning nil discards that provider's diagnostics.
func WithStderrFor(factory func(host.Backend) io.Writer) Option {
	return func(config *config) { config.stderrFor = factory }
}

// Default returns the native adapter set with conventional executable names.
// Hosts can replace an adapter by passing another implementation with the
// same Backend to durableacp.WithAdapters.
func Default(options ...Option) []host.Adapter {
	settings := config{}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	stderr := func(backend host.Backend) []acpx.Option {
		if settings.stderrFor == nil {
			return nil
		}
		return []acpx.Option{acpx.WithStderr(settings.stderrFor(backend))}
	}
	return []host.Adapter{
		claude.New(stderr(claude.Backend)...),
		codex.New(stderr(codex.Backend)...),
		cursor.New(stderr(cursor.Backend)...),
		antigravity.New(stderr(antigravity.Backend)...),
	}
}
