package journal

import (
	"encoding/json"
	"strings"
)

type ReplayMatcher struct {
	records []replayKey
	next    int
	stream  replaySequence
	pending *replayKey
}

type replayKey struct {
	session      string
	conversation string
	thread       string
	turn         uint64
	event        string
	occurrence   uint64
}

type replayFields struct {
	session      string
	conversation string
	thread       string
	turn         string
	event        string
	source       string
}

type replayDefaults struct {
	conversations  map[string]string
	threads        map[string]string
	sessionThreads map[string]string
}

type replaySequence struct {
	defaults replayDefaults
	scopes   map[string]*replayScope
}

type replayScope struct {
	turn         uint64
	providerTurn string
	open         bool
	occurrences  map[string]uint64
	sources      map[string]replaySource
}

type replaySource struct {
	id         string
	occurrence uint64
}

func NewReplayMatcher(records []Record) *ReplayMatcher {
	defaults := replayDefaultsFor(records)
	canonical := replaySequence{defaults: defaults}
	matcher := &ReplayMatcher{stream: replaySequence{defaults: defaults}}
	for _, record := range records {
		if key, ok := canonical.key(record); ok {
			matcher.records = append(matcher.records, key)
		}
	}
	return matcher
}

func (m *ReplayMatcher) Match(record Record) bool {
	if m == nil {
		return false
	}
	key, ok := m.stream.key(record)
	if !ok {
		m.pending = nil
		return false
	}
	m.pending = &key
	for index := m.next; index < len(m.records); index++ {
		if m.records[index] != key {
			continue
		}
		m.next = index + 1
		m.pending = nil
		return true
	}
	return false
}

func (m *ReplayMatcher) Reset() {
	if m == nil {
		return
	}
	m.next = 0
	m.stream.reset()
	m.pending = nil
}

func (m *ReplayMatcher) Record(record Record) {
	if m == nil {
		return
	}
	if m.pending != nil {
		m.records = append(m.records, *m.pending)
		m.pending = nil
		return
	}
	key, ok := m.stream.key(record)
	if ok {
		m.records = append(m.records, key)
	}
}

func (s *replaySequence) reset() {
	s.scopes = nil
}

func (s *replaySequence) key(record Record) (replayKey, bool) {
	fields, ok := replayFieldsFor(record)
	if !ok {
		return replayKey{}, false
	}
	fields = s.defaults.apply(fields)
	scopeKey := strings.Join([]string{fields.session, fields.conversation, fields.thread}, "\x00")
	if s.scopes == nil {
		s.scopes = map[string]*replayScope{}
	}
	scope := s.scopes[scopeKey]
	if scope == nil {
		scope = &replayScope{}
		s.scopes[scopeKey] = scope
	}
	scope.observeTurn(fields)
	occurrence := scope.nextOccurrence(fields.event, fields.source)
	key := replayKey{
		session: fields.session, conversation: fields.conversation, thread: fields.thread,
		turn: scope.turn, event: fields.event, occurrence: occurrence,
	}
	if replayTerminal(fields.event) {
		scope.open = false
		scope.providerTurn = ""
	}
	return key, true
}

func (s *replayScope) observeTurn(fields replayFields) {
	if fields.event == EventUserMessage {
		s.startTurn("")
		return
	}
	if fields.event == EventAgentTurnStarted {
		return
	}
	if !s.open {
		s.startTurn(fields.turn)
		return
	}
	if fields.turn == "" {
		return
	}
	if s.providerTurn == "" {
		s.providerTurn = fields.turn
		return
	}
	if s.providerTurn != fields.turn {
		s.startTurn(fields.turn)
	}
}

func (s *replayScope) startTurn(providerTurn string) {
	s.turn++
	s.providerTurn = providerTurn
	s.open = true
	s.occurrences = nil
	s.sources = nil
}

func (s *replayScope) nextOccurrence(event, source string) uint64 {
	if s.occurrences == nil {
		s.occurrences = map[string]uint64{}
	}
	if s.sources == nil {
		s.sources = map[string]replaySource{}
	}
	if source != "" {
		if previous := s.sources[event]; previous.id == source {
			return previous.occurrence
		}
	}
	s.occurrences[event]++
	occurrence := s.occurrences[event]
	s.sources[event] = replaySource{id: source, occurrence: occurrence}
	return occurrence
}

func replayDefaultsFor(records []Record) replayDefaults {
	defaults := replayDefaults{
		conversations: map[string]string{}, threads: map[string]string{}, sessionThreads: map[string]string{},
	}
	conversationValues := map[string]map[string]struct{}{}
	fields := make([]replayFields, 0, len(records))
	for _, record := range records {
		field, ok := replayFieldsFor(record)
		if !ok {
			continue
		}
		fields = append(fields, field)
		if field.conversation != "" {
			addReplayDefaultValue(conversationValues, field.session, field.conversation)
		}
	}
	for key, values := range conversationValues {
		defaults.conversations[key] = soleReplayDefault(values)
	}
	threadValues := map[string]map[string]struct{}{}
	sessionThreadValues := map[string]map[string]struct{}{}
	for _, field := range fields {
		field = defaults.applyConversation(field)
		if field.thread == "" {
			continue
		}
		addReplayDefaultValue(threadValues, replayConversationScope(field.session, field.conversation), field.thread)
		addReplayDefaultValue(sessionThreadValues, field.session, field.thread)
	}
	for key, values := range threadValues {
		defaults.threads[key] = soleReplayDefault(values)
	}
	for key, values := range sessionThreadValues {
		defaults.sessionThreads[key] = soleReplayDefault(values)
	}
	return defaults
}

func (d replayDefaults) apply(fields replayFields) replayFields {
	fields = d.applyConversation(fields)
	if fields.thread == "" {
		fields.thread = d.threads[replayConversationScope(fields.session, fields.conversation)]
		if fields.thread == "" {
			fields.thread = d.sessionThreads[fields.session]
		}
	}
	return fields
}

func (d replayDefaults) applyConversation(fields replayFields) replayFields {
	if fields.conversation == "" {
		fields.conversation = d.conversations[fields.session]
	}
	return fields
}

func replayConversationScope(session, conversation string) string {
	return session + "\x00" + conversation
}

func addReplayDefaultValue(values map[string]map[string]struct{}, key, value string) {
	if values[key] == nil {
		values[key] = map[string]struct{}{}
	}
	values[key][value] = struct{}{}
}

func soleReplayDefault(values map[string]struct{}) string {
	if len(values) != 1 {
		return ""
	}
	for value := range values {
		return value
	}
	return ""
}

func replayFieldsFor(record Record) (replayFields, bool) {
	if !replayEvent(record.Event) {
		return replayFields{}, false
	}
	var data map[string]any
	if json.Unmarshal(record.Data, &data) != nil {
		return replayFields{}, false
	}
	return replayFields{
		session: strings.TrimSpace(record.SessionID), conversation: strings.TrimSpace(record.Conversation),
		thread: replayAgentString(data, "backend_thread_id"), turn: strings.TrimSpace(record.TurnID),
		event: strings.TrimSpace(record.Event), source: strings.TrimSpace(record.SourceEventID),
	}, true
}

func replayEvent(event string) bool {
	switch event {
	case EventUserMessage, EventAgentMessage, EventAgentPlanProposed, EventAgentTodoUpdated,
		EventAgentWorkspace, EventAgentPermission, EventAgentTurnStarted, EventAgentYielded,
		EventAgentInterrupted, EventAgentTurnFailed, EventAgentProcessExited:
		return true
	default:
		return false
	}
}

func replayTerminal(event string) bool {
	switch event {
	case EventAgentYielded, EventAgentInterrupted, EventAgentTurnFailed, EventAgentProcessExited:
		return true
	default:
		return false
	}
}

func replayAgentString(data map[string]any, key string) string {
	agent, _ := data["agent"].(map[string]any)
	value, _ := agent[key].(string)
	return strings.TrimSpace(value)
}
