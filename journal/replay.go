package journal

import (
	"encoding/json"
	"strings"
)

type ReplayMatcher struct {
	records        []replayKey
	completedTurns []replayTurn
	next           int
	limit          int
}

type replayKey struct {
	session      string
	conversation string
	thread       string
	turn         string
	event        string
	providerID   string
	content      string
	source       string
}

type replayTurn struct {
	session      string
	conversation string
	thread       string
	turn         string
}

func NewReplayMatcher(records []Record) *ReplayMatcher {
	matcher := &ReplayMatcher{}
	for _, record := range records {
		matcher.append(record)
	}
	matcher.limit = len(matcher.records)
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
	if key.providerID != "" {
		for index := m.next; index < m.limit; index++ {
			persisted := m.records[index]
			if persisted.sameStream(key) && persisted.providerID != "" && persisted.providerID == key.providerID {
				m.next = index + 1
				return true
			}
		}
	}
	if key.content != "" {
		for index := m.next; index < m.limit; index++ {
			persisted := m.records[index]
			if persisted.sameStream(key) && persisted.content == key.content {
				m.next = index + 1
				return true
			}
		}
	}
	return false
}

// MatchesCompletedTurn reports whether record belongs to a turn whose durable terminal event is already present.
func (m *ReplayMatcher) MatchesCompletedTurn(record Record) bool {
	if m == nil {
		return false
	}
	key, ok := replayKeyFor(record)
	if !ok || key.turn == "" {
		return false
	}
	for _, completed := range m.completedTurns {
		if completed.matches(key) {
			return true
		}
	}
	return false
}

func (m *ReplayMatcher) Reset() {
	if m == nil {
		return
	}
	m.next = 0
	m.limit = len(m.records)
}

func (m *ReplayMatcher) Record(record Record) {
	if m != nil {
		m.append(record)
	}
}

func (m *ReplayMatcher) append(record Record) {
	key, ok := replayKeyFor(record)
	if !ok {
		return
	}
	if key.source != "" && len(m.records) > 0 {
		last := len(m.records) - 1
		if m.records[last].source == key.source && m.records[last].sameStream(key) {
			m.records[last] = key
			return
		}
	}
	m.records = append(m.records, key)
	if replayTerminal(key.event) && key.turn != "" {
		m.completedTurns = append(m.completedTurns, replayTurn{
			session: key.session, conversation: key.conversation, thread: key.thread, turn: key.turn,
		})
	}
}

func (a replayKey) sameStream(b replayKey) bool {
	return a.event == b.event && replayScopeMatches(a.session, b.session) &&
		replayScopeMatches(a.conversation, b.conversation) && replayScopeMatches(a.thread, b.thread)
}

func replayScopeMatches(a, b string) bool {
	return a == "" || b == "" || a == b
}

func (a replayTurn) matches(b replayKey) bool {
	return a.turn == b.turn && replayScopeMatches(a.session, b.session) &&
		replayScopeMatches(a.conversation, b.conversation) && replayScopeMatches(a.thread, b.thread)
}

func replayKeyFor(record Record) (replayKey, bool) {
	var data map[string]any
	if json.Unmarshal(record.Data, &data) != nil {
		return replayKey{}, false
	}
	key := replayKey{
		session: strings.TrimSpace(record.SessionID), conversation: strings.TrimSpace(record.Conversation),
		thread: replayAgentString(data, "backend_thread_id"), turn: strings.TrimSpace(record.TurnID), event: strings.TrimSpace(record.Event),
		providerID: replayString(data, "provider_event_id"), source: strings.TrimSpace(record.SourceEventID),
	}
	switch key.event {
	case EventUserMessage, EventAgentMessage, EventAgentPlanProposed:
		key.content = replayString(data, "message")
	case EventAgentTodoUpdated:
		key.content = replayJSON(data["todos"])
	case EventAgentWorkspace:
		key.content = replayJSON(data["selected"])
	case EventAgentPermission:
		key.content = replayJSON(data["selected"])
	case EventAgentTurnStarted, EventAgentYielded, EventAgentInterrupted, EventAgentTurnFailed, EventAgentProcessExited:
		key.content = strings.TrimSpace(record.TurnID)
	default:
		return replayKey{}, false
	}
	return key, key.providerID != "" || key.content != ""
}

func replayString(data map[string]any, key string) string {
	value, _ := data[key].(string)
	return strings.TrimSpace(value)
}

func replayJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(raw)
}

func replayAgentString(data map[string]any, key string) string {
	agent, _ := data["agent"].(map[string]any)
	return replayString(agent, key)
}

func replayTerminal(event string) bool {
	switch event {
	case EventAgentYielded, EventAgentInterrupted, EventAgentTurnFailed, EventAgentProcessExited:
		return true
	default:
		return false
	}
}
