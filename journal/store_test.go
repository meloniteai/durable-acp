package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestStoreAppendReadSessionsAndPermissions(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "conversations")
	now := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	nextID := 0
	store, err := NewStore(dir,
		WithNow(func() time.Time { return now }),
		WithNewID(func() string {
			nextID++
			return fmt.Sprintf("event-%d", nextID)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var notified []Record
	store.Subscribe(func(record Record) { notified = append(notified, record) })

	first, err := store.Append(Record{SessionID: " session/one ", Event: " user.message "})
	if err != nil {
		t.Fatal(err)
	}
	if first.Schema != defaultSchemaID || first.SchemaVersion != schemaVersion || first.EventVersion != defaultEventVersion {
		t.Fatalf("defaults = %#v", first)
	}
	if first.EventID != "event-1" || first.Sequence != 1 || !first.Timestamp.Equal(now) {
		t.Fatalf("assigned envelope = %#v", first)
	}
	if first.SessionID != "session/one" || first.Event != "user.message" || string(first.Data) != "{}" {
		t.Fatalf("normalized record = %#v", first)
	}

	second, err := store.Append(Record{SessionID: "session/one", Event: "agent.yielded", Data: json.RawMessage(`{"ok":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 2 || second.EventID != "event-2" {
		t.Fatalf("second record = %#v", second)
	}
	if !reflect.DeepEqual(notified, []Record{first, second}) {
		t.Fatalf("listener records = %#v", notified)
	}

	records, err := store.Read("session/one", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, []Record{first}) {
		t.Fatalf("through records = %#v", records)
	}
	sessions, err := store.Sessions()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sessions, []string{"session_one"}) {
		t.Fatalf("sessions = %#v", sessions)
	}

	fileInfo, err := os.Stat(filepath.Join(dir, "session_one.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %o", fileInfo.Mode().Perm())
	}
	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %o", dirInfo.Mode().Perm())
	}
}

func TestStoreSchemaValidation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir, WithSchemaID("example.journal.v1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.Append(Record{SessionID: "one", Event: "agent.message", Schema: "other.journal.v1"}); err == nil {
		t.Fatal("Append accepted an unsupported schema")
	}
	if _, err := store.Append(Record{SessionID: "one", Event: "agent.message", SchemaVersion: 2}); err == nil {
		t.Fatal("Append accepted an unsupported schema version")
	}

	bad := completeRecord("other.journal.v1", 1, "one", "agent.message", json.RawMessage(`{}`))
	writeJournal(t, filepath.Join(dir, "one.jsonl"), bad)
	if _, err := store.Read("one", 0, 0); err == nil {
		t.Fatal("Read accepted an unsupported schema")
	}
}

func TestStoreRejectsCollidingSessionFilenames(t *testing.T) {
	t.Parallel()

	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if _, appendErr := store.Append(Record{SessionID: "session/one", Event: "agent.message"}); appendErr != nil {
		t.Fatal(appendErr)
	}
	if _, appendErr := store.Append(Record{SessionID: "session_one", Event: "agent.message"}); appendErr == nil {
		t.Fatal("Append mixed two session IDs that map to the same journal filename")
	}
	if _, readErr := store.Read("session_one", 0, 0); readErr == nil {
		t.Fatal("Read returned another session's colliding journal")
	}
	records, err := store.Read("session/one", 0, 0)
	if err != nil || len(records) != 1 || records[0].SessionID != "session/one" {
		t.Fatalf("original session records = %#v, %v", records, err)
	}
}

func TestStoreLastSequence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, openErr := NewStore(dir)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = store.Close() })

	if _, sequenceErr := store.LastSequence(""); sequenceErr == nil {
		t.Fatal("LastSequence accepted a missing session")
	}
	if _, sequenceErr := store.LastSequence("missing"); !errors.Is(sequenceErr, os.ErrNotExist) {
		t.Fatalf("missing LastSequence error = %v", sequenceErr)
	}
	for range 2 {
		if _, appendErr := store.Append(Record{SessionID: "one", Event: "agent.message"}); appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	sequence, err := store.LastSequence("one")
	if err != nil || sequence != 2 {
		t.Fatalf("open writer sequence = %d, %v", sequence, err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	sequence, err = store.LastSequence("one")
	if err != nil || sequence != 2 {
		t.Fatalf("persisted sequence = %d, %v", sequence, err)
	}
	if writeErr := os.WriteFile(filepath.Join(dir, "empty.jsonl"), nil, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	sequence, err = store.LastSequence("empty")
	if err != nil || sequence != 0 {
		t.Fatalf("empty sequence = %d, %v", sequence, err)
	}
}

func TestStoreCursorReadsAndTail(t *testing.T) {
	t.Parallel()

	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	want := make([]Record, 5)
	for i := range want {
		want[i], err = store.Append(Record{
			SessionID: "one",
			Event:     "agent.message",
			Data:      json.RawMessage(fmt.Sprintf(`{"index":%d}`, i+1)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	after, err := store.ReadAfter("one", 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, want[3:]) {
		t.Fatalf("records after cursor = %#v, want %#v", after, want[3:])
	}

	bounded, err := store.Read("one", 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bounded, want[1:4]) {
		t.Fatalf("bounded records = %#v, want %#v", bounded, want[1:4])
	}

	empty, err := store.Read("one", 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty cursor window = %#v", empty)
	}
	if _, boundsErr := store.Read("one", 5, 4); boundsErr == nil {
		t.Fatal("Read accepted reversed cursor bounds")
	}

	tail, err := store.Tail("one", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tail, want[3:]) {
		t.Fatalf("tail records = %#v, want %#v", tail, want[3:])
	}
	all, err := store.Tail("one", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(all, want) {
		t.Fatalf("long tail = %#v, want %#v", all, want)
	}
	if _, limitErr := store.Tail("one", 0); limitErr == nil {
		t.Fatal("Tail accepted a non-positive limit")
	}
}

func TestStoreReducerReplaysAndNormalizes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	reducer := mergeStateReducer(t)
	store, err := NewStore(dir, WithReducer(reducer))
	if err != nil {
		t.Fatal(err)
	}

	if _, appendErr := store.Append(Record{SessionID: "one", Event: "host.state", Data: json.RawMessage(`{"activity":"running"}`)}); appendErr != nil {
		t.Fatal(appendErr)
	}
	presentation := &Presentation{Surfaces: []string{"conversation"}, Label: "State", Attention: true}
	second, err := store.Append(Record{
		SourceEventID: "source-2",
		SessionID:     "one",
		Event:         "host.state",
		Data:          json.RawMessage(`{"mode":"loop"}`),
		Presentation:  presentation,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, second.Data, `{"activity":"running","mode":"loop"}`)
	if second.SourceEventID != "source-2" || !reflect.DeepEqual(second.Presentation, presentation) {
		t.Fatalf("reduced envelope = %#v", second)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	store, err = NewStore(dir, WithReducer(reducer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	third, err := store.Append(Record{SessionID: "one", Event: "host.state", Data: json.RawMessage(`{"attention":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	if third.Sequence != 3 {
		t.Fatalf("sequence = %d, want 3", third.Sequence)
	}
	assertJSONEqual(t, third.Data, `{"activity":"running","mode":"loop","attention":true}`)
}

func TestStoreReaderHandlesTornTailAndRejectsEarlierProblems(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	valid := completeRecord(defaultSchemaID, 1, "torn", "user.message", json.RawMessage(`{}`))
	line := marshalLine(t, valid)
	if writeErr := os.WriteFile(filepath.Join(dir, "torn.jsonl"), append(line, []byte(`{"schema":`)...), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	records, err := store.Read("torn", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, []Record{valid}) {
		t.Fatalf("torn records = %#v", records)
	}

	badBody := append(marshalLine(t, completeRecord(defaultSchemaID, 1, "bad", "user.message", json.RawMessage(`{}`))), []byte("{not json}\n")...)
	if err := os.WriteFile(filepath.Join(dir, "bad.jsonl"), badBody, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read("bad", 0, 0); err == nil {
		t.Fatal("Read accepted malformed non-tail JSON")
	}

	first := completeRecord(defaultSchemaID, 2, "order", "user.message", json.RawMessage(`{}`))
	second := completeRecord(defaultSchemaID, 1, "order", "agent.message", json.RawMessage(`{}`))
	if err := os.WriteFile(filepath.Join(dir, "order.jsonl"), append(marshalLine(t, first), marshalLine(t, second)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read("order", 0, 1); err == nil {
		t.Fatal("Read accepted non-monotonic records")
	}
}

func TestStoreAppendRepairsTornTail(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, appendErr := store.Append(Record{SessionID: "torn", Event: "user.message"}); appendErr != nil {
		t.Fatal(appendErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	path := filepath.Join(dir, "torn.jsonl")
	// #nosec G304 -- path is inside the test temporary directory.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, writeErr := file.WriteString(`{"partial":`); writeErr != nil {
		_ = file.Close()
		t.Fatal(writeErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	store, err = NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	second, err := store.Append(Record{SessionID: "torn", Event: "agent.message"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 2 {
		t.Fatalf("sequence = %d, want 2", second.Sequence)
	}
	records, err := store.Read("torn", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Sequence != 1 || records[1].Sequence != 2 {
		t.Fatalf("records = %#v", records)
	}
}

func TestStoreReopensWriterAfterAppendFailure(t *testing.T) {
	t.Parallel()

	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, appendErr := store.Append(Record{SessionID: "retry", Event: "user.message"}); appendErr != nil {
		t.Fatal(appendErr)
	}
	store.mu.Lock()
	closeErr := store.writers["retry"].file.Close()
	store.mu.Unlock()
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if _, appendErr := store.Append(Record{SessionID: "retry", Event: "agent.message"}); appendErr == nil {
		t.Fatal("Append accepted a closed writer")
	}
	store.mu.Lock()
	_, retained := store.writers["retry"]
	store.mu.Unlock()
	if retained {
		t.Fatal("failed writer was retained")
	}
	retried, err := store.Append(Record{SessionID: "retry", Event: "agent.message"})
	if err != nil {
		t.Fatal(err)
	}
	if retried.Sequence != 2 {
		t.Fatalf("sequence = %d, want 2", retried.Sequence)
	}
}

func TestStoreReducerFailureAndValidation(t *testing.T) {
	t.Parallel()

	if _, err := NewStore(""); err == nil {
		t.Fatal("NewStore accepted an empty directory")
	}
	if _, err := NewStore(t.TempDir(), WithSchemaID("")); err == nil {
		t.Fatal("NewStore accepted an empty schema ID")
	}
	store, err := NewStore(t.TempDir(), WithReducer(func(json.RawMessage, Record) (json.RawMessage, Record, error) {
		return nil, Record{}, errors.New("nope")
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Append(Record{SessionID: "one", Event: "agent.message"}); err == nil {
		t.Fatal("Append accepted a reducer error")
	}
	if _, err := store.Append(Record{}); err == nil {
		t.Fatal("Append accepted a missing session and event")
	}
	if _, err := store.Read("", 0, 0); err == nil {
		t.Fatal("Read accepted a missing session")
	}
}

func mergeStateReducer(t *testing.T) Reducer {
	t.Helper()
	return func(state json.RawMessage, record Record) (json.RawMessage, Record, error) {
		if record.Event != "host.state" {
			return state, record, nil
		}
		merged := map[string]any{}
		if len(state) > 0 {
			if err := json.Unmarshal(state, &merged); err != nil {
				return nil, Record{}, err
			}
		}
		var patch map[string]any
		if err := json.Unmarshal(record.Data, &patch); err != nil {
			return nil, Record{}, err
		}
		maps.Copy(merged, patch)
		raw, err := json.Marshal(merged)
		if err != nil {
			return nil, Record{}, err
		}
		record.Data = raw
		return raw, record, nil
	}
}

func completeRecord(schema string, sequence uint64, sessionID, event string, data json.RawMessage) Record {
	return Record{
		Schema: schema, SchemaVersion: schemaVersion, EventVersion: defaultEventVersion,
		EventID: "event", Sequence: sequence, Timestamp: time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC),
		SessionID: sessionID, Event: event, Data: data,
	}
}

func marshalLine(t *testing.T, record Record) []byte {
	t.Helper()
	line, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return append(line, '\n')
}

func writeJournal(t *testing.T, path string, records ...Record) {
	t.Helper()
	var body []byte
	for _, record := range records {
		body = append(body, marshalLine(t, record)...)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertJSONEqual(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatal(err)
	}
	var wantValue any
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON = %#v, want %#v", gotValue, wantValue)
	}
}
