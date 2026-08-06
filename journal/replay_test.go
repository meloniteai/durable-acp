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

func TestReplayMatcherMatchesDifferentProviderIDsWithinStableTurns(t *testing.T) {
	existing := []Record{
		replayMessage(EventUserMessage, "", "msg-user-1", "same"),
		replayMessage(EventAgentMessage, "thread:1", "msg-agent-1", "same"),
		replayMessage(EventUserMessage, "thread:1", "msg-user-2", "same"),
		replayMessage(EventAgentMessage, "thread:2", "msg-agent-2", "same"),
	}
	matcher := NewReplayMatcher(existing)
	replayed := []Record{
		replayMessage(EventUserMessage, "thread:1", "item-1", "same"),
		replayMessage(EventAgentMessage, "thread:1", "item-2", "same"),
		replayMessage(EventUserMessage, "thread:2", "item-3", "same"),
		replayMessage(EventAgentMessage, "thread:2", "item-4", "same"),
	}
	for index, record := range replayed {
		if !matcher.Match(record) {
			t.Fatalf("replay record %d did not match", index)
		}
	}
	if matcher.Match(replayMessage(EventAgentMessage, "thread:3", "item-5", "same")) {
		t.Fatal("new turn matched exhausted repeated content")
	}
}

func TestReplayMatcherMissDoesNotDiscardRemainingHistory(t *testing.T) {
	existing := []Record{
		replayMessage(EventAgentMessage, "thread:1", "msg-1", "first"),
		replayMessage(EventAgentMessage, "thread:1", "msg-2", "second"),
	}
	matcher := NewReplayMatcher(existing)
	miss := replayMessage(EventAgentMessage, "thread:1", "item-new", "new")
	if matcher.Match(miss) {
		t.Fatal("new replay event matched existing history")
	}
	matcher.Record(miss)
	if !matcher.Match(replayMessage(EventAgentMessage, "thread:1", "item-2", "second")) {
		t.Fatal("a miss discarded the remaining persisted history")
	}
}

func replayMessage(event, turnID, providerID, message string) Record {
	payload := struct {
		Message         string `json:"message"`
		ProviderEventID string `json:"provider_event_id"`
		Agent           struct {
			BackendThreadID string `json:"backend_thread_id"`
		} `json:"agent"`
	}{Message: message, ProviderEventID: providerID}
	payload.Agent.BackendThreadID = "thread"
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return Record{SessionID: "host", Conversation: "coder", Event: event, TurnID: turnID, Data: data}
}
