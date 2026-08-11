package journal

import (
	"encoding/json"
	"testing"
)

func TestReplayMatcherMatchesPartialSuffixByProviderID(t *testing.T) {
	matcher := NewReplayMatcher([]Record{
		replayMessage(EventUserMessage, "turn-1", "user-1", "first question"),
		replayMessage(EventAgentMessage, "turn-1", "agent-1", "first answer"),
		replayMessage(EventUserMessage, "turn-2", "user-2", "second question"),
		replayMessage(EventAgentMessage, "turn-2", "agent-2", "second answer"),
	})
	for index, record := range []Record{
		replayMessage(EventUserMessage, "replay-2", "user-2", "reformatted question"),
		replayMessage(EventAgentMessage, "replay-2", "agent-2", "reformatted answer"),
	} {
		if !matcher.Match(record) {
			t.Fatalf("suffix record %d did not match", index)
		}
	}
}

func TestReplayMatcherFallsBackToContent(t *testing.T) {
	matcher := NewReplayMatcher([]Record{
		replayMessage(EventUserMessage, "live", "user-live", "question"),
		replayMessage(EventAgentMessage, "live", "agent-live", "answer"),
		replayMessage(EventAgentTodoUpdated, "live", "todo-live", "next step"),
	})
	for index, record := range []Record{
		replayMessage(EventUserMessage, "replay", "user-replay", "question"),
		replayMessage(EventAgentMessage, "replay", "agent-replay", "answer"),
		replayMessage(EventAgentTodoUpdated, "replay", "todo-replay", "next step"),
	} {
		if !matcher.Match(record) {
			t.Fatalf("content record %d did not match", index)
		}
	}
}

func TestReplayMatcherTreatsDifferentIdentityAndContentAsNew(t *testing.T) {
	existing := []Record{
		replayMessage(EventAgentMessage, "turn", "message-1", "first"),
		replayMessage(EventAgentPlanProposed, "turn", "plan-1", "plan"),
		replayMessage(EventAgentMessage, "turn", "message-2", "second"),
	}
	matcher := NewReplayMatcher(existing)
	if !matcher.Match(replayMessage(EventAgentMessage, "replay", "message-1", "changed first")) {
		t.Fatal("stable provider ID did not match changed content")
	}
	newRecord := replayMessage(EventAgentMessage, "replay", "message-new", "new")
	if matcher.Match(newRecord) {
		t.Fatal("different identity and content matched")
	}
	matcher.Record(newRecord)
	if !matcher.Match(replayMessage(EventAgentPlanProposed, "replay", "plan-1", "changed plan")) {
		t.Fatal("a miss discarded the remaining persisted plan")
	}
	if !matcher.Match(replayMessage(EventAgentMessage, "replay", "replacement-id", "second")) {
		t.Fatal("a miss discarded the remaining persisted message")
	}
	matcher.Reset()
	if !matcher.Match(newRecord) {
		t.Fatal("recorded miss was unavailable on the next replay")
	}
}

func TestReplayMatcherRecognizesCompletedPersistedTurn(t *testing.T) {
	matcher := NewReplayMatcher([]Record{
		replayMessage(EventAgentMessage, "turn-1", "message-1", "answer"),
		replayMessage(EventAgentYielded, "turn-1", "", ""),
	})
	if !matcher.MatchesCompletedTurn(replayMessage(EventUserMessage, "turn-1", "missing", "old prompt")) {
		t.Fatal("unmatched replay in a completed turn was not recognized")
	}
	if matcher.MatchesCompletedTurn(replayMessage(EventUserMessage, "turn-2", "new", "new prompt")) {
		t.Fatal("new replay turn matched a completed durable turn")
	}
}

func TestReplayMatcherPreservesRepeatedContent(t *testing.T) {
	matcher := NewReplayMatcher([]Record{
		replayMessage(EventAgentMessage, "turn-1", "message-1", "same"),
		replayMessage(EventAgentMessage, "turn-2", "message-2", "same"),
	})
	for index := range 2 {
		if !matcher.Match(replayMessage(EventAgentMessage, "replay", "new-id", "same")) {
			t.Fatalf("repeated record %d did not match", index)
		}
	}
	if matcher.Match(replayMessage(EventAgentMessage, "replay", "new-id", "same")) {
		t.Fatal("content matched beyond its persisted occurrences")
	}
}

func TestReplayMatcherScopesIdentity(t *testing.T) {
	matcher := NewReplayMatcher([]Record{replayMessage(EventAgentMessage, "turn", "message", "same")})
	replay := replayMessage(EventAgentMessage, "replay", "message", "same")
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
	first := replayMessage(EventAgentMessage, "turn", "message-1", "a")
	first.SourceEventID = "stream-1"
	snapshot := replayMessage(EventAgentMessage, "turn", "message-1", "ab")
	snapshot.SourceEventID = "stream-1"
	second := replayMessage(EventAgentMessage, "turn", "message-2", "second")
	second.SourceEventID = "stream-2"
	matcher := NewReplayMatcher([]Record{first, snapshot, second})
	if !matcher.Match(replayMessage(EventAgentMessage, "replay", "replayed-1", "ab")) {
		t.Fatal("replayed stream did not match its final snapshot")
	}
	if !matcher.Match(replayMessage(EventAgentMessage, "replay", "replayed-2", "second")) {
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
