package journal

import (
	"encoding/json"
	"testing"
)

func TestReplayMatcherUsesOrderedSemanticContent(t *testing.T) {
	existing := []Record{
		{Conversation: "host", Event: EventAgentPlanProposed, Data: json.RawMessage(`{"message":"same plan"}`)},
		{Conversation: "host", Event: EventAgentPlanProposed, Data: json.RawMessage(`{"message":"same plan"}`)},
	}
	matcher := NewReplayMatcher(existing)
	for _, providerEventID := range []string{"plan-1", "plan-2"} {
		replay := Record{Conversation: "host", Event: EventAgentPlanProposed, Data: json.RawMessage(`{"message":"same plan","provider_event_id":"` + providerEventID + `"}`)}
		if !matcher.Match(replay) {
			t.Fatalf("ordered record did not match replay %q", providerEventID)
		}
	}
	replay := Record{Conversation: "host", Event: EventAgentPlanProposed, Data: json.RawMessage(`{"message":"same plan","provider_event_id":"plan-3"}`)}
	if matcher.Match(replay) {
		t.Fatal("third replay matched only two records")
	}
	matcher.Record(replay)
	matcher.Reset()
	for _, providerEventID := range []string{"plan-1", "plan-2", "plan-3"} {
		replay := Record{Conversation: "host", Event: EventAgentPlanProposed, Data: json.RawMessage(`{"message":"same plan","provider_event_id":"` + providerEventID + `"}`)}
		if !matcher.Match(replay) {
			t.Fatalf("recorded history did not match replay %q after reset", providerEventID)
		}
	}
}
