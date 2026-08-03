package acpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/host"
)

const (
	configCategoryModel     = "model"
	configCategoryMode      = "mode"
	configCategoryReasoning = "thought_level"
)

// ConfigOption is one selectable ACP session configuration value.
type ConfigOption struct {
	ID          string
	Label       string
	Description string
}

// SessionConfig is the useful subset of an ACP session configOptions response. ACP
// agents use different option IDs, but consistently categorize model, mode,
// and thought-level selections.
type SessionConfig struct {
	Models           []ConfigOption
	Modes            []ConfigOption
	Reasoning        []ConfigOption
	CurrentModel     string
	CurrentMode      string
	CurrentReasoning string
}

// SessionConfigurationClient performs the two ACP configuration operations.
type SessionConfigurationClient interface {
	SetConfigOption(context.Context, string, string, string) (json.RawMessage, error)
	SetMode(context.Context, string, string) error
}

// SessionConfigurator applies generic selections to one concrete ACP session.
type SessionConfigurator struct {
	mu        sync.Mutex
	sessionID string
	options   []acp.SessionConfigOption
	modes     *acp.SessionModeState
	model     string
	reasoning string
	mode      string
}

// NewSessionConfigurator creates configuration state from an ACP session response.
func NewSessionConfigurator(sessionID string, raw json.RawMessage) *SessionConfigurator {
	configurator := &SessionConfigurator{}
	configurator.Update(sessionID, raw)
	return configurator
}

// Update merges configuration fields present in an ACP session response.
func (c *SessionConfigurator) Update(sessionID string, raw json.RawMessage) {
	if c == nil {
		return
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	c.mu.Lock()
	if value := strings.TrimSpace(sessionID); value != "" {
		c.sessionID = value
	}
	if optionsRaw, ok := fields["configOptions"]; ok {
		var options []acp.SessionConfigOption
		if json.Unmarshal(optionsRaw, &options) == nil {
			c.options = cloneOptions(options)
		}
	}
	if modesRaw, ok := fields["modes"]; ok {
		var modes *acp.SessionModeState
		if json.Unmarshal(modesRaw, &modes) == nil {
			c.modes = cloneModes(modes)
		}
	}
	c.mu.Unlock()
}

// SetDesired merges non-empty generic selections into the desired configuration.
func (c *SessionConfigurator) SetDesired(model, reasoning, mode string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if value := strings.TrimSpace(model); value != "" {
		c.model = value
	}
	if value := strings.TrimSpace(reasoning); value != "" {
		c.reasoning = value
	}
	if value := strings.TrimSpace(mode); value != "" {
		c.mode = value
	}
	c.mu.Unlock()
}

// Desired returns the last non-empty generic selections.
func (c *SessionConfigurator) Desired() (string, string, string) {
	if c == nil {
		return "", "", ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.model, c.reasoning, c.mode
}

// Current returns the configuration advertised as effective by the ACP session.
func (c *SessionConfigurator) Current() SessionConfig {
	if c == nil {
		return SessionConfig{}
	}
	c.mu.Lock()
	options := cloneOptions(c.options)
	modes := cloneModes(c.modes)
	c.mu.Unlock()
	return sessionConfigFromACP(options, modes)
}

// SetCurrentMode records an ACP current_mode_update notification.
func (c *SessionConfigurator) SetCurrentMode(mode string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.modes != nil {
		c.modes.CurrentModeId = acp.SessionModeId(strings.TrimSpace(mode))
		c.mu.Unlock()
		return
	}
	for index := range c.options {
		option := c.options[index].Select
		if option != nil && matchesConfigOption(option, "mode", "permission_mode") {
			option.CurrentValue = acp.SessionConfigValueId(strings.TrimSpace(mode))
			break
		}
	}
	c.mu.Unlock()
}

// Apply changes known generic controls whose effective values differ.
func (c *SessionConfigurator) Apply(ctx context.Context, client SessionConfigurationClient, model, reasoning, mode string) (bool, error) {
	if c == nil || client == nil {
		return false, errors.New("acpx: session configuration client is required")
	}
	c.SetDesired(model, reasoning, mode)
	model, reasoning, mode = c.Desired()
	changed := false
	applied, err := c.applyConfigOption(ctx, client, model, "model")
	changed = changed || applied
	if err != nil {
		return changed, err
	}
	applied, err = c.applyConfigOption(ctx, client, reasoning, "thought_level", "reasoning", "reasoning_effort", "effort")
	changed = changed || applied
	if err != nil {
		return changed, err
	}
	applied, err = c.applyMode(ctx, client, mode)
	return changed || applied, err
}

func (c *SessionConfigurator) applyConfigOption(ctx context.Context, client SessionConfigurationClient, value string, keys ...string) (bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return false, nil
	}
	c.mu.Lock()
	option := matchingConfigOption(c.options, keys...)
	sessionID := c.sessionID
	c.mu.Unlock()
	if option == nil || string(option.CurrentValue) == value {
		return false, nil
	}
	raw, err := client.SetConfigOption(ctx, sessionID, string(option.Id), value)
	if err != nil {
		return false, fmt.Errorf("acpx: set session config %q to %q: %w", option.Id, value, err)
	}
	options, modes, err := decodeConfigResponse(raw)
	if err != nil {
		return false, err
	}
	c.replace(sessionID, options, modes)
	return true, nil
}

func (c *SessionConfigurator) applyMode(ctx context.Context, client SessionConfigurationClient, value string) (bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return false, nil
	}
	c.mu.Lock()
	modes := cloneModes(c.modes)
	sessionID := c.sessionID
	c.mu.Unlock()
	if modes != nil {
		if !sessionModeAvailable(modes, value) || string(modes.CurrentModeId) == value {
			return false, nil
		}
		if err := client.SetMode(ctx, sessionID, value); err != nil {
			return false, fmt.Errorf("acpx: set session mode to %q: %w", value, err)
		}
		c.SetCurrentMode(value)
		return true, nil
	}
	c.mu.Lock()
	option := matchingConfigOption(c.options, "mode", "permission_mode")
	c.mu.Unlock()
	if option == nil || !sessionConfigValueAvailable(option, value) {
		return false, nil
	}
	return c.applyConfigOption(ctx, client, value, "mode", "permission_mode")
}

func (c *SessionConfigurator) replace(sessionID string, options []acp.SessionConfigOption, modes *acp.SessionModeState) {
	c.mu.Lock()
	if value := strings.TrimSpace(sessionID); value != "" {
		c.sessionID = value
	}
	c.options = cloneOptions(options)
	if modes != nil {
		c.modes = cloneModes(modes)
	}
	c.mu.Unlock()
}

func (c *SessionConfigurator) state() ([]acp.SessionConfigOption, *acp.SessionModeState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneOptions(c.options), cloneModes(c.modes)
}

func decodeSessionConfiguration(raw json.RawMessage) ([]acp.SessionConfigOption, *acp.SessionModeState) {
	var payload struct {
		ConfigOptions []acp.SessionConfigOption `json:"configOptions"`
		Modes         *acp.SessionModeState     `json:"modes"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil, nil
	}
	return payload.ConfigOptions, payload.Modes
}

func decodeConfigResponse(raw json.RawMessage) ([]acp.SessionConfigOption, *acp.SessionModeState, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, fmt.Errorf("acpx: decode session configuration response: %w", err)
	}
	if _, ok := fields["configOptions"]; !ok {
		return nil, nil, errors.New("acpx: session configuration response omitted configOptions")
	}
	options, modes := decodeSessionConfiguration(raw)
	return options, modes, nil
}

func sessionConfigFromACP(options []acp.SessionConfigOption, modes *acp.SessionModeState) SessionConfig {
	raw, err := json.Marshal(map[string]any{"configOptions": options})
	if err != nil {
		return SessionConfig{}
	}
	config := ParseConfig(raw)
	if modes == nil {
		return config
	}
	seen := make(map[string]bool, len(config.Modes))
	for _, option := range config.Modes {
		seen[option.ID] = true
	}
	for _, mode := range modes.AvailableModes {
		id := string(mode.Id)
		if seen[id] {
			continue
		}
		description := ""
		if mode.Description != nil {
			description = *mode.Description
		}
		config.Modes = append(config.Modes, ConfigOption{ID: id, Label: mode.Name, Description: description})
	}
	if modes.CurrentModeId != "" {
		config.CurrentMode = string(modes.CurrentModeId)
	}
	return config
}

func matchingConfigOption(options []acp.SessionConfigOption, keys ...string) *acp.SessionConfigOptionSelect {
	for index := range options {
		option := options[index].Select
		if option == nil || !matchesConfigOption(option, keys...) {
			continue
		}
		cloned := *option
		return &cloned
	}
	return nil
}

func matchesConfigOption(option *acp.SessionConfigOptionSelect, keys ...string) bool {
	id := string(option.Id)
	category := ""
	if option.Category != nil {
		category = string(*option.Category)
	}
	for _, key := range keys {
		if id == key || category == key {
			return true
		}
	}
	return false
}

func sessionModeAvailable(modes *acp.SessionModeState, value string) bool {
	for _, mode := range modes.AvailableModes {
		if string(mode.Id) == value {
			return true
		}
	}
	return false
}

func sessionConfigValueAvailable(option *acp.SessionConfigOptionSelect, value string) bool {
	if option.Options.Ungrouped != nil {
		for _, candidate := range *option.Options.Ungrouped {
			if string(candidate.Value) == value {
				return true
			}
		}
	}
	if option.Options.Grouped != nil {
		for _, group := range *option.Options.Grouped {
			for _, candidate := range group.Options {
				if string(candidate.Value) == value {
					return true
				}
			}
		}
	}
	return false
}

func cloneModes(modes *acp.SessionModeState) *acp.SessionModeState {
	if modes == nil {
		return nil
	}
	cloned := *modes
	cloned.AvailableModes = append([]acp.SessionMode(nil), modes.AvailableModes...)
	return &cloned
}

func cloneOptions(options []acp.SessionConfigOption) []acp.SessionConfigOption {
	if options == nil {
		return nil
	}
	raw, err := json.Marshal(options)
	if err != nil {
		return nil
	}
	var cloned []acp.SessionConfigOption
	if json.Unmarshal(raw, &cloned) != nil {
		return nil
	}
	return cloned
}

// ParseConfig extracts model, permission-mode, and reasoning selections from
// a JSON-RPC ACP response such as session/new or session/set_config_option.
// Invalid or unfamiliar config options are ignored so one agent extension
// cannot make an otherwise usable session unavailable.
func ParseConfig(raw json.RawMessage) SessionConfig {
	var payload struct {
		ConfigOptions []struct {
			ID           string          `json:"id"`
			Category     string          `json:"category"`
			CurrentValue json.RawMessage `json:"currentValue"`
			Options      []struct {
				Value       string `json:"value"`
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"options"`
		} `json:"configOptions"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return SessionConfig{}
	}
	var config SessionConfig
	for _, option := range payload.ConfigOptions {
		values := make([]ConfigOption, 0, len(option.Options))
		for _, value := range option.Options {
			if strings.TrimSpace(value.Value) == "" {
				continue
			}
			values = append(values, ConfigOption{ID: value.Value, Label: value.Name, Description: value.Description})
		}
		var current string
		_ = json.Unmarshal(option.CurrentValue, &current)
		current = strings.TrimSpace(current)
		switch {
		case option.Category == configCategoryModel || option.ID == configCategoryModel:
			config.Models, config.CurrentModel = values, current
		case option.Category == configCategoryMode || option.ID == configCategoryMode:
			config.Modes, config.CurrentMode = values, current
		case option.Category == configCategoryReasoning:
			config.Reasoning, config.CurrentReasoning = values, current
		}
	}
	return config
}

// BackendModels converts ACP selections into host catalog models.
func BackendModels(options []ConfigOption) []host.BackendModel {
	result := make([]host.BackendModel, 0, len(options))
	for _, option := range options {
		result = append(result, host.BackendModel{ID: option.ID, Label: option.Label})
	}
	return result
}

// PermissionModes converts ACP mode selections into host catalog modes.
func PermissionModes(options []ConfigOption) []host.BackendPermissionMode {
	result := make([]host.BackendPermissionMode, 0, len(options))
	for _, option := range options {
		result = append(result, host.BackendPermissionMode{ID: option.ID, Label: option.Label, Description: option.Description})
	}
	return result
}

// ReasoningLevels converts ACP thought-level selections into host catalog
// reasoning levels.
func ReasoningLevels(options []ConfigOption) []host.BackendReasoning {
	result := make([]host.BackendReasoning, 0, len(options))
	for _, option := range options {
		result = append(result, host.BackendReasoning{ID: option.ID, Label: option.Label, Description: option.Description})
	}
	return result
}
