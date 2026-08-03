package acpx

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type sessionConfigurationClientStub struct {
	raw         json.RawMessage
	configCalls []string
	modeCalls   []string
}

func (c *sessionConfigurationClientStub) SetConfigOption(
	_ context.Context,
	_ string,
	configID, value string,
) (json.RawMessage, error) {
	c.configCalls = append(c.configCalls, configID+"="+value)
	var payload map[string]any
	if err := json.Unmarshal(c.raw, &payload); err != nil {
		return nil, err
	}
	candidates, ok := payload["configOptions"].([]any)
	if !ok {
		return nil, errors.New("configOptions missing")
	}
	for _, candidate := range candidates {
		option, ok := candidate.(map[string]any)
		if !ok {
			return nil, errors.New("invalid config option")
		}
		if option["id"] == configID {
			option["currentValue"] = value
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	c.raw = raw
	return raw, nil
}

func (c *sessionConfigurationClientStub) SetMode(
	_ context.Context,
	_ string,
	mode string,
) error {
	c.modeCalls = append(c.modeCalls, mode)
	return nil
}

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

func TestSessionConfiguratorAppliesChangedControls(t *testing.T) {
	raw := json.RawMessage(`{
		"configOptions":[
			{"id":"model","category":"model","type":"select","currentValue":"model-a","options":[{"value":"model-a","name":"A"},{"value":"model-b","name":"B"}]},
			{"id":"effort","category":"thought_level","type":"select","currentValue":"high","options":[{"value":"high","name":"High"},{"value":"low","name":"Low"}]}
		],
		"modes":{"currentModeId":"ask","availableModes":[{"id":"ask","name":"Ask"},{"id":"plan","name":"Plan"}]}
	}`)
	configurator := NewSessionConfigurator("provider-1", raw)
	configurator.Update("provider-2", json.RawMessage(`{"sessionId":"provider-2"}`))
	current := configurator.Current()
	if current.CurrentModel != "model-a" || current.CurrentReasoning != "high" || current.CurrentMode != "ask" || len(current.Modes) != 2 {
		t.Fatalf("initial configuration = %+v", current)
	}
	client := &sessionConfigurationClientStub{raw: raw}
	changed, err := configurator.Apply(context.Background(), client, " model-b ", " low ", " plan ")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || strings.Join(client.configCalls, ",") != "model=model-b,effort=low" || strings.Join(client.modeCalls, ",") != "plan" {
		t.Fatalf("calls = %v / %v, changed=%v", client.configCalls, client.modeCalls, changed)
	}
	current = configurator.Current()
	if current.CurrentModel != "model-b" || current.CurrentReasoning != "low" || current.CurrentMode != "plan" {
		t.Fatalf("effective configuration = %+v", current)
	}
	changed, err = configurator.Apply(context.Background(), client, "", "", "")
	if err != nil || changed || len(client.configCalls) != 2 || len(client.modeCalls) != 1 {
		t.Fatalf("unchanged apply = %v, %v / %v", err, client.configCalls, client.modeCalls)
	}
}

func TestSessionConfiguratorSkipsUnknownOptionalMode(t *testing.T) {
	raw := json.RawMessage(`{"configOptions":[{"id":"mode","category":"mode","type":"select","currentValue":"ask","options":[{"value":"ask","name":"Ask"},{"value":"auto","name":"Auto"}]}]}`)
	configurator := NewSessionConfigurator("provider-1", raw)
	client := &sessionConfigurationClientStub{raw: raw}
	changed, err := configurator.Apply(context.Background(), client, "", "", "unknown")
	if err != nil || changed || len(client.configCalls) != 0 {
		t.Fatalf("unknown mode apply = %v, calls=%v", err, client.configCalls)
	}
	changed, err = configurator.Apply(context.Background(), client, "", "", "auto")
	if err != nil || !changed || strings.Join(client.configCalls, ",") != "mode=auto" {
		t.Fatalf("known mode apply = %v, changed=%v calls=%v", err, changed, client.configCalls)
	}
	configurator.SetCurrentMode("ask")
	if current := configurator.Current().CurrentMode; current != "ask" {
		t.Fatalf("current mode = %q", current)
	}
}

func TestNilSessionConfigurator(t *testing.T) {
	var configurator *SessionConfigurator
	configurator.Update("provider", nil)
	configurator.SetDesired("model", "reasoning", "mode")
	configurator.SetCurrentMode("mode")
	if model, reasoning, mode := configurator.Desired(); model != "" || reasoning != "" || mode != "" {
		t.Fatalf("nil desired = %q %q %q", model, reasoning, mode)
	}
	if current := configurator.Current(); len(current.Models) != 0 || len(current.Modes) != 0 || len(current.Reasoning) != 0 || current.CurrentModel != "" || current.CurrentMode != "" || current.CurrentReasoning != "" {
		t.Fatalf("nil current = %+v", current)
	}
	if _, err := configurator.Apply(context.Background(), nil, "", "", ""); err == nil {
		t.Fatal("nil configurator apply succeeded")
	}
}

func optionIDs(options []ConfigOption) string {
	ids := make([]string, 0, len(options))
	for _, option := range options {
		ids = append(ids, option.ID)
	}
	return strings.Join(ids, ",")
}
