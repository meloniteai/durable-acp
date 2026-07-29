package acpx

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseConfigBucketsSelections(t *testing.T) {
	raw := json.RawMessage(`{"configOptions":[
		{"id":"mode","category":"mode","currentValue":"auto","options":[
			{"value":"auto","name":"Auto","description":"classifier"},
			{"value":"default","name":"Default"},
			{"value":"","name":"skip me"}
		]},
		{"id":"model","category":"model","currentValue":"sonnet","options":[
			{"value":"opus[1m]","name":"Opus"},
			{"value":"sonnet","name":"Sonnet"}
		]},
		{"id":"effort","category":"thought_level","currentValue":"high","options":[
			{"value":"low","name":"Low"},
			{"value":"high","name":"High"}
		]},
		{"id":"agent","currentValue":"default","options":[{"value":"default","name":"Default"}]}
	]}`)
	config := ParseConfig(raw)
	if got := optionIDs(config.Models); got != "opus[1m],sonnet" || config.CurrentModel != "sonnet" {
		t.Fatalf("models = %q current=%q", got, config.CurrentModel)
	}
	if got := optionIDs(config.Modes); got != "auto,default" || config.CurrentMode != "auto" {
		t.Fatalf("modes = %q current=%q", got, config.CurrentMode)
	}
	if got := optionIDs(config.Reasoning); got != "low,high" || config.CurrentReasoning != "high" {
		t.Fatalf("reasoning = %q current=%q", got, config.CurrentReasoning)
	}
	if config.Models[0].Label != "Opus" {
		t.Fatalf("label = %#v", config.Models[0])
	}
	models := BackendModels(config.Models)
	if len(models) != 2 || models[0].ID != "opus[1m]" {
		t.Fatalf("models catalog = %#v", models)
	}
	if modes := PermissionModes(config.Modes); len(modes) != 2 || modes[0].Description != "classifier" {
		t.Fatalf("modes catalog = %#v", modes)
	}
	if reasoning := ReasoningLevels(config.Reasoning); len(reasoning) != 2 || reasoning[1].ID != "high" {
		t.Fatalf("reasoning catalog = %#v", reasoning)
	}
}

func TestParseConfigIgnoresInvalidInput(t *testing.T) {
	if got := ParseConfig(json.RawMessage(`not JSON`)); len(got.Models) != 0 || len(got.Modes) != 0 || len(got.Reasoning) != 0 {
		t.Fatalf("config = %#v", got)
	}
	config := ParseConfig(json.RawMessage(`{"configOptions":[{"id":"model","category":"model","currentValue":"haiku","options":[{"value":"haiku","name":"Haiku"}]}]}`))
	if len(config.Reasoning) != 0 || config.CurrentReasoning != "" {
		t.Fatalf("reasoning = %#v / %q", config.Reasoning, config.CurrentReasoning)
	}
}

func optionIDs(options []ConfigOption) string {
	ids := make([]string, 0, len(options))
	for _, option := range options {
		ids = append(ids, option.ID)
	}
	return strings.Join(ids, ",")
}
