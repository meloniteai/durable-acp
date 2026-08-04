package journal

import (
	"encoding/json"
	"strings"
)

type ReplayMatcher struct {
	records []replayKey
	next    int
}

type replayKey struct {
	conversation string
	event        string
	providerID   string
	content      string
}

func NewReplayMatcher(records []Record) *ReplayMatcher {
	matcher := &ReplayMatcher{}
	for _, record := range records {
		if key, ok := replayKeyFor(record); ok {
			matcher.records = append(matcher.records, key)
		}
	}
	return matcher
}

func (m *ReplayMatcher) Match(record Record) bool {
	if m == nil {
		return false
	}
	key, ok := replayKeyFor(record)
	if !ok {
		return false
	}
	for index := m.next; index < len(m.records); index++ {
		if !m.records[index].equal(key) {
			continue
		}
		m.next = index + 1
		return true
	}
	return false
}

func (m *ReplayMatcher) Reset() {
	if m != nil {
		m.next = 0
	}
}

func (m *ReplayMatcher) Record(record Record) {
	if m == nil {
		return
	}
	key, ok := replayKeyFor(record)
	if !ok {
		return
	}
	m.records = append(m.records, key)
	m.next = len(m.records)
}

func (k replayKey) equal(other replayKey) bool {
	if k.event != other.event || k.conversation != "" && other.conversation != "" && k.conversation != other.conversation {
		return false
	}
	if k.providerID != "" && other.providerID != "" {
		return k.providerID == other.providerID
	}
	return k.content == other.content
}

func replayKeyFor(record Record) (replayKey, bool) {
	var data map[string]any
	if json.Unmarshal(record.Data, &data) != nil {
		return replayKey{}, false
	}
	key := replayKey{
		conversation: record.Conversation,
		event:        record.Event,
		providerID:   replayString(data, "provider_event_id"),
	}
	switch record.Event {
	case EventUserMessage, EventAgentMessage, EventAgentPlanProposed:
		key.content = strings.TrimSpace(replayString(data, "message"))
	case EventAgentTodoUpdated:
		key.content = replayJSON(data["todos"])
	case EventAgentWorkspace:
		key.content = replaySelectedJSON(data, "changed", "change_count", "file_count", "files", "kind", "path", "paths", "summary")
	case EventAgentPermission:
		key.content = replaySelectedJSON(data, "behavior", "phase", "request_id")
	case EventAgentTurnStarted, EventAgentYielded, EventAgentInterrupted, EventAgentTurnFailed, EventAgentProcessExited:
		key.content = strings.TrimSpace(record.TurnID)
	default:
		return replayKey{}, false
	}
	return key, key.content != ""
}

func replaySelectedJSON(data map[string]any, keys ...string) string {
	selected := make(map[string]any, len(keys))
	for _, key := range keys {
		if value, ok := data[key]; ok {
			selected[key] = value
		}
	}
	return replayJSON(selected)
}

func replayString(data map[string]any, key string) string {
	value, _ := data[key].(string)
	return value
}

func replayJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(raw)
}
