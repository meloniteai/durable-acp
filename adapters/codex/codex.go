// Package codex provides the bundled Codex ACP adapter.
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

const forkPromptMethod = "codex/fork_prompt"

// Backend is the stable backend name used in Engine session manifests.
const Backend host.Backend = "codex"

// Adapter drives a Codex-compatible ACP sidecar.
type Adapter struct{ *acpx.Adapter }

// New creates an adapter for the standard codex-acp executable. Use
// acpx.WithCommand when a host installs it elsewhere.
func New(options ...acpx.Option) *Adapter {
	capabilities := acp.ClientCapabilities{Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}}
	return &Adapter{Adapter: acpx.New(acpx.Config{
		Backend:                 Backend,
		Command:                 "codex-acp",
		ClientCapabilities:      &capabilities,
		InitializeFields:        map[string]any{"capabilities": map[string]any{}},
		ClientCapabilityFields:  map[string]any{"plan": map[string]any{}},
		LoadSessionFirst:        true,
		RestartOnExit:           true,
		LegacyExtensions:        true,
		BestEffortConfiguration: true,
		SessionModeValues:       []string{"plan"},
	}, options...)}
}

func (a *Adapter) ForkPrompt(ctx context.Context, request host.ForkPromptRequest) (host.ForkPromptResponse, error) {
	if strings.TrimSpace(request.SessionID) == "" {
		return host.ForkPromptResponse{}, errors.New("codex: session ID is required")
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return host.ForkPromptResponse{}, errors.New("codex: fork prompt is required")
	}
	raw, err := a.CallSession(ctx, request.SessionID, forkPromptMethod, map[string]any{
		"prompt":     request.Prompt,
		"mcpServers": request.MCPServers,
	})
	if err != nil {
		return host.ForkPromptResponse{}, err
	}
	var response host.ForkPromptResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return host.ForkPromptResponse{}, fmt.Errorf("codex: fork response: %w", err)
	}
	return response, nil
}
