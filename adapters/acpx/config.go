package acpx

import (
	"encoding/json"
	"strings"

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
