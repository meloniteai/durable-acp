package journal

import (
	"encoding/json"
	"testing"
)

func TestReplayMatcherUsesOrderedTurnOccurrences(t *testing.T) {
	existing := []Record{
		replayMessage(EventUserMessage, "", "msg-user-1", "same"),
		replayMessage(EventAgentMessage, "thread:1", "msg-agent-1", "same"),
		replayMessage(EventUserMessage, "thread:1", "msg-user-2", "same"),
		replayMessage(EventAgentMessage, "thread:2", "msg-agent-2", "same"),
	}
	matcher := NewReplayMatcher(existing)
	replayed := []Record{
		replayMessage(EventUserMessage, "replay-turn-a", "item-1", "same"),
		replayMessage(EventAgentMessage, "replay-turn-a", "item-2", "same"),
		replayMessage(EventUserMessage, "replay-turn-b", "item-3", "same"),
		replayMessage(EventAgentMessage, "replay-turn-b", "item-4", "same"),
	}
	for index, record := range replayed {
		if !matcher.Match(record) {
			t.Fatalf("replay record %d did not match", index)
		}
	}
	third := []Record{
		replayMessage(EventUserMessage, "replay-turn-c", "item-5", "same"),
		replayMessage(EventAgentMessage, "replay-turn-c", "item-6", "same"),
	}
	for _, record := range third {
		if matcher.Match(record) {
			t.Fatal("new turn matched exhausted history")
		}
		matcher.Record(record)
	}
	matcher.Reset()
	for index, record := range append(replayed, third...) {
		if !matcher.Match(record) {
			t.Fatalf("record %d did not match after a second replay", index)
		}
	}
	if matcher.Match(replayMessage(EventUserMessage, "replay-turn-d", "item-7", "same")) {
		t.Fatal("new turn matched exhausted history")
	}
}

func TestReplayMatcherIdentityDoesNotDependOnProviderIDOrContent(t *testing.T) {
	existing := []Record{
		replayMessage(EventUserMessage, "live-turn", "msg-user", "original question"),
		replayMessage(EventAgentMessage, "live-turn", "msg-agent", "original answer"),
	}
	matcher := NewReplayMatcher(existing)
	for index, record := range []Record{
		replayMessage(EventUserMessage, "replay-turn", "item-user", "reformatted question"),
		replayMessage(EventAgentMessage, "replay-turn", "item-agent", "reformatted answer"),
	} {
		if !matcher.Match(record) {
			t.Fatalf("replay record %d depended on provider ID or content", index)
		}
	}
}

func TestReplayMatcherMissDoesNotDiscardRemainingHistory(t *testing.T) {
	existing := []Record{
		replayMessage(EventAgentMessage, "thread:1", "msg-1", "first"),
		replayMessage(EventAgentPlanProposed, "thread:1", "plan-1", "plan"),
		replayMessage(EventAgentMessage, "thread:1", "msg-2", "second"),
	}
	matcher := NewReplayMatcher(existing)
	if !matcher.Match(replayMessage(EventAgentMessage, "replay-turn", "item-1", "first")) {
		t.Fatal("first replay event did not match")
	}
	miss := replayMessage(EventAgentTodoUpdated, "replay-turn", "item-new", "new")
	if matcher.Match(miss) {
		t.Fatal("new event kind matched existing history")
	}
	matcher.Record(miss)
	if !matcher.Match(replayMessage(EventAgentPlanProposed, "replay-turn", "item-plan", "changed plan")) {
		t.Fatal("a miss discarded the remaining persisted plan")
	}
	if !matcher.Match(replayMessage(EventAgentMessage, "replay-turn", "item-2", "second")) {
		t.Fatal("a miss discarded the remaining persisted message")
	}
}

func TestReplayMatcherScopesOccurrenceIdentity(t *testing.T) {
	existing := []Record{replayMessage(EventAgentMessage, "thread:1", "msg-1", "same")}
	matcher := NewReplayMatcher(existing)
	replay := replayMessage(EventAgentMessage, "thread:1", "item-1", "same")
	var data map[string]any
	if err := json.Unmarshal(replay.Data, &data); err != nil {
		t.Fatal(err)
	}
	agent, ok := data["agent"].(map[string]any)
	if !ok {
		t.Fatalf("agent data = %#v", data["agent"])
	}
	agent["backend_thread_id"] = "other-thread"
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	replay.Data = raw
	if matcher.Match(replay) {
		t.Fatal("event from another backend thread matched")
	}
}

func TestReplayMatcherGroupsCumulativeSourceSnapshots(t *testing.T) {
	first := replayMessage(EventAgentMessage, "thread:1", "msg-1", "a")
	first.SourceEventID = "stream-1"
	snapshot := replayMessage(EventAgentMessage, "thread:1", "msg-1", "ab")
	snapshot.SourceEventID = "stream-1"
	second := replayMessage(EventAgentMessage, "thread:1", "msg-2", "second")
	second.SourceEventID = "stream-2"
	matcher := NewReplayMatcher([]Record{first, snapshot, second})
	if !matcher.Match(replayMessage(EventAgentMessage, "replay-turn", "item-1", "ab")) {
		t.Fatal("replayed stream did not match cumulative snapshots")
	}
	if !matcher.Match(replayMessage(EventAgentMessage, "replay-turn", "item-2", "second")) {
		t.Fatal("event after cumulative snapshots did not match")
	}
}

func replayMessage(event, turnID, providerID, message string) Record {
	payload := struct {
		Message         string `json:"message"`
		ProviderEventID string `json:"provider_event_id"`
		Todos           []any  `json:"todos,omitempty"`
		Agent           struct {
			BackendThreadID string `json:"backend_thread_id"`
		} `json:"agent"`
	}{Message: message, ProviderEventID: providerID}
	if event == EventAgentTodoUpdated {
		payload.Todos = []any{message}
	}
	payload.Agent.BackendThreadID = "thread"
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return Record{SessionID: "host", Conversation: "coder", Event: event, TurnID: turnID, Data: data}
}
