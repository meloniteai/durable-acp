package acpx

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/host"
)

var ErrUnsupportedOperation = errors.New("unsupported adapter operation")

// UnsupportedOperationError identifies a direct provider operation that is unavailable.
type UnsupportedOperationError struct {
	Backend   host.Backend
	Operation string
}

func (e *UnsupportedOperationError) Error() string {
	return fmt.Sprintf("acpx: backend %q does not support %s", e.Backend, e.Operation)
}

func (e *UnsupportedOperationError) Unwrap() error {
	return ErrUnsupportedOperation
}

// CallSession invokes a provider extension for an active session.
func (a *Adapter) CallSession(ctx context.Context, sessionID, method string, params map[string]any) ([]byte, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return nil, err
	}
	if !a.config.LegacyExtensions && !strings.HasPrefix(method, "_") {
		return nil, &UnsupportedOperationError{Backend: a.Backend(), Operation: method}
	}
	payload := make(map[string]any, len(params)+1)
	maps.Copy(payload, params)
	if _, ok := payload["sessionId"]; !ok {
		payload["sessionId"] = managed.backendID
	}
	return managed.conn.CallProvider(ctx, method, payload)
}

// SessionDirectory returns the working directory of an active session.
func (a *Adapter) SessionDirectory(sessionID string) (string, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return "", err
	}
	return managed.worktree, nil
}

// BackendSessionID returns the provider session ID for an active host session.
func (a *Adapter) BackendSessionID(sessionID string) (string, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return "", err
	}
	return managed.backendID, nil
}

// ActiveTurnID returns the active provider turn ID, if any.
func (a *Adapter) ActiveTurnID(sessionID string) (string, error) {
	managed, err := a.session(sessionID)
	if err != nil {
		return "", err
	}
	return managed.currentTurnID(), nil
}

// StopSessionProcess closes an active session connection without removing its resumable state.
func (a *Adapter) StopSessionProcess(sessionID string) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	return managed.conn.Close()
}

// PromptSession sends a prompt to a provider session using an existing connection.
func (a *Adapter) PromptSession(ctx context.Context, sessionID, backendSessionID, prompt string) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	backendSessionID = strings.TrimSpace(backendSessionID)
	if backendSessionID == "" {
		return errors.New("acpx: backend session ID is required")
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return errors.New("acpx: prompt is required")
	}
	_, err = managed.conn.Prompt(ctx, &acp.PromptRequest{SessionId: acp.SessionId(backendSessionID), Prompt: []acp.ContentBlock{acp.TextBlock(prompt)}})
	return err
}

// PromptForkSession runs an isolated provider session and limits automatic approvals to its MCP servers.
func (a *Adapter) PromptForkSession(ctx context.Context, sessionID, backendSessionID, prompt string, servers []host.ForkMCPServer) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	backendSessionID = strings.TrimSpace(backendSessionID)
	policy := &forkInteractionPolicy{tools: map[string]string{}}
	for _, server := range servers {
		name := strings.TrimSpace(server.Name)
		if name != "" {
			policy.allowedToolPrefixes = append(policy.allowedToolPrefixes, "mcp__"+name+"__")
		}
	}
	managed.forkMu.Lock()
	managed.forks[backendSessionID] = policy
	managed.forkMu.Unlock()
	defer func() {
		managed.forkMu.Lock()
		delete(managed.forks, backendSessionID)
		managed.forkMu.Unlock()
	}()
	return a.PromptSession(ctx, sessionID, backendSessionID, prompt)
}

// CloseBackendSession closes a provider session using an existing connection.
func (a *Adapter) CloseBackendSession(ctx context.Context, sessionID, backendSessionID string) error {
	managed, err := a.session(sessionID)
	if err != nil {
		return err
	}
	_, err = managed.conn.CloseSession(ctx, &acp.CloseSessionRequest{SessionId: acp.SessionId(strings.TrimSpace(backendSessionID))})
	return err
}
