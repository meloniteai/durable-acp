// Package claude provides the bundled Claude ACP adapter.
package claude

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/adapters/acpx"
	"github.com/meloniteai/durable-acp/host"
)

const forkMethod = "session/fork"

// Backend is the stable backend name used in Engine session manifests.
const Backend host.Backend = "claude"

// Adapter drives a Claude-compatible ACP sidecar.
type Adapter struct{ *acpx.Adapter }

// New creates an adapter for the standard claude-agent-acp executable. Use
// acpx.WithCommand when a host installs it elsewhere.
func New(options ...acpx.Option) *Adapter {
	return &Adapter{Adapter: acpx.New(acpx.Config{
		Backend:                 Backend,
		Command:                 "claude-agent-acp",
		ClientCapabilities:      &acp.ClientCapabilities{},
		LoadSessionFirst:        true,
		RestartOnExit:           true,
		LegacyExtensions:        true,
		BestEffortConfiguration: true,
	}, options...)}
}

func (a *Adapter) ForkPrompt(ctx context.Context, request host.ForkPromptRequest) (host.ForkPromptResponse, error) {
	if strings.TrimSpace(request.SessionID) == "" {
		return host.ForkPromptResponse{}, errors.New("claude: session ID is required")
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return host.ForkPromptResponse{}, errors.New("claude: fork prompt is required")
	}
	directory, err := a.SessionDirectory(request.SessionID)
	if err != nil {
		return host.ForkPromptResponse{}, err
	}
	servers := make([]map[string]any, 0, len(request.MCPServers))
	for _, server := range request.MCPServers {
		args := make([]string, len(server.Args))
		copy(args, server.Args)
		environment := make([]host.ForkMCPEnv, len(server.Env))
		copy(environment, server.Env)
		servers = append(servers, map[string]any{
			"type": server.Type, "name": server.Name, "command": server.Command,
			"args": args, "env": environment,
			"url": server.URL, "headers": append([]host.ForkMCPHTTPHeader(nil), server.Headers...),
		})
	}
	params := map[string]any{"cwd": directory, "mcpServers": servers}
	if instructions := strings.TrimSpace(request.Instructions); instructions != "" {
		params["_meta"] = map[string]any{"systemPrompt": instructions}
	}
	raw, err := a.CallSession(ctx, request.SessionID, forkMethod, params)
	if err != nil {
		return host.ForkPromptResponse{}, fmt.Errorf("claude ACP session/fork: %w", err)
	}
	var response struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return host.ForkPromptResponse{}, fmt.Errorf("claude: fork response: %w", err)
	}
	response.SessionID = strings.TrimSpace(response.SessionID)
	if response.SessionID == "" {
		return host.ForkPromptResponse{}, errors.New("claude: fork response missing session ID")
	}
	if response.SessionID == strings.TrimSpace(request.SessionID) {
		return host.ForkPromptResponse{}, errors.New("claude ACP session/fork returned an invalid child session id")
	}
	go a.runFork(context.WithoutCancel(ctx), request.SessionID, response.SessionID, request.Prompt, request.MCPServers)
	return host.ForkPromptResponse{Accepted: true}, nil
}

func (a *Adapter) runFork(parent context.Context, parentID, childID, prompt string, servers []host.ForkMCPServer) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Minute)
	defer cancel()
	_ = a.PromptForkSession(ctx, parentID, childID, prompt, servers)
	closeCtx, closeCancel := context.WithTimeout(parent, 10*time.Second)
	defer closeCancel()
	_ = a.CloseBackendSession(closeCtx, parentID, childID)
}
