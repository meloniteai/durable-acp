package journal

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultSchemaID      = "durable-acp.journal.v1"
	schemaVersion        = 1
	defaultEventVersion  = 1
	journalFileExtension = ".jsonl"
)

// Reducer maintains opaque per-session state and can normalize a record before
// it is persisted. The store replays persisted records through the reducer
// whenever it opens a writer, so reducers must be deterministic for records
// they have already normalized.
type Reducer func(state json.RawMessage, record Record) (nextState json.RawMessage, persisted Record, err error)

// Option configures a Store.
type Option func(*config)

type config struct {
	schemaID string
	now      func() time.Time
	newID    func() string
	reducer  Reducer
}

// WithSchemaID changes the schema identifier accepted and assigned by a Store.
func WithSchemaID(schemaID string) Option {
	return func(config *config) {
		config.schemaID = strings.TrimSpace(schemaID)
	}
}

// WithNow supplies timestamps for records that do not provide one.
func WithNow(now func() time.Time) Option {
	return func(config *config) {
		if now != nil {
			config.now = now
		}
	}
}

// WithNewID supplies event IDs for records that do not provide one.
func WithNewID(newID func() string) Option {
	return func(config *config) {
		if newID != nil {
			config.newID = newID
		}
	}
}

// WithReducer installs an optional per-session reducer.
func WithReducer(reducer Reducer) Option {
	return func(config *config) {
		config.reducer = reducer
	}
}

// Store owns open writers for a directory of per-session JSONL journals.
type Store struct {
	mu        sync.Mutex
	dir       string
	schemaID  string
	writers   map[string]*writer
	listeners []func(Record)
	now       func() time.Time
	newID     func() string
	reducer   Reducer
}

type writer struct {
	file  *os.File
	seq   uint64
	state json.RawMessage
}

// NewStore creates a Store rooted at dir. The directory is created with owner
// only permissions when needed.
func NewStore(dir string, options ...Option) (*Store, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("journal: directory is required")
	}

	settings := config{
		schemaID: defaultSchemaID,
		now:      func() time.Time { return time.Now().UTC() },
		newID:    newEventID,
	}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if settings.schemaID == "" {
		return nil, errors.New("journal: schema ID is required")
	}

	dir = filepath.Clean(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("journal: create directory %s: %w", dir, err)
	}
	// #nosec G302 -- journal directories need owner-only traversal permissions.
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("journal: secure directory %s: %w", dir, err)
	}

	return &Store{
		dir:      dir,
		schemaID: settings.schemaID,
		writers:  map[string]*writer{},
		now:      settings.now,
		newID:    settings.newID,
		reducer:  settings.reducer,
	}, nil
}

// Subscribe registers a listener called after a record has been durably
// appended. Listeners run without the Store lock.
func (s *Store) Subscribe(listener func(Record)) {
	if listener == nil {
		return
	}
	s.mu.Lock()
	s.listeners = append(s.listeners, listener)
	s.mu.Unlock()
}

// Append assigns envelope defaults, durably appends record, and returns the
// persisted record.
func (s *Store) Append(record Record) (Record, error) {
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.Event = strings.TrimSpace(record.Event)
	if record.SessionID == "" || record.Event == "" {
		return Record{}, errors.New("journal: session_id and event are required")
	}
	if record.Schema == "" {
		record.Schema = s.schemaID
	}
	if record.Schema != s.schemaID {
		return Record{}, fmt.Errorf("journal: unsupported schema %q", record.Schema)
	}
	if record.SchemaVersion == 0 {
		record.SchemaVersion = schemaVersion
	}
	if record.SchemaVersion != schemaVersion {
		return Record{}, fmt.Errorf("journal: unsupported schema version %d", record.SchemaVersion)
	}
	if record.EventVersion == 0 {
		record.EventVersion = defaultEventVersion
	}
	if record.EventID == "" {
		record.EventID = s.newID()
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = s.now()
	}
	if len(record.Data) == 0 {
		record.Data = json.RawMessage(`{}`)
	}

	s.mu.Lock()
	writer, err := s.writerLocked(record.SessionID)
	if err != nil {
		s.mu.Unlock()
		return Record{}, err
	}

	record.Sequence = writer.seq + 1
	nextState := writer.state
	if s.reducer != nil {
		expectedSessionID := record.SessionID
		expectedEventID := record.EventID
		expectedEventVersion := record.EventVersion
		expectedTimestamp := record.Timestamp
		reducedState, normalized, reduceErr := s.reducer(writer.state, record)
		if reduceErr != nil {
			s.mu.Unlock()
			return Record{}, fmt.Errorf("journal: reduce %s: %w", record.SessionID, reduceErr)
		}
		nextState = cloneRawMessage(reducedState)
		record = normalized
		record.Schema = s.schemaID
		record.SchemaVersion = schemaVersion
		record.EventVersion = expectedEventVersion
		record.EventID = expectedEventID
		record.Sequence = writer.seq + 1
		record.Timestamp = expectedTimestamp
		record.SessionID = strings.TrimSpace(record.SessionID)
		record.Event = strings.TrimSpace(record.Event)
		if record.SessionID == "" || record.Event == "" {
			s.mu.Unlock()
			return Record{}, errors.New("journal: reducer returned a record without session_id or event")
		}
		if record.SessionID != expectedSessionID {
			s.mu.Unlock()
			return Record{}, errors.New("journal: reducer changed record session_id")
		}
		if len(record.Data) == 0 {
			record.Data = json.RawMessage(`{}`)
		}
	}

	line, err := json.Marshal(record)
	if err == nil {
		line = append(line, '\n')
		var written int
		written, err = writer.file.Write(line)
		if err == nil && written != len(line) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = writer.file.Sync()
	}
	if err == nil {
		writer.seq = record.Sequence
		writer.state = nextState
	}
	listeners := append([]func(Record){}, s.listeners...)
	s.mu.Unlock()
	if err != nil {
		return Record{}, fmt.Errorf("journal: append %s: %w", record.SessionID, err)
	}
	for _, listener := range listeners {
		listener(record)
	}
	return record, nil
}

// Read returns records for sessionID through the inclusive sequence boundary.
// A through value of zero returns every record.
func (s *Store) Read(sessionID string, through uint64) ([]Record, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, errors.New("journal: session_id is required")
	}

	s.mu.Lock()
	if writer := s.writers[sessionID]; writer != nil {
		if err := writer.file.Sync(); err != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("journal: sync %s: %w", sessionID, err)
		}
	}
	s.mu.Unlock()

	return readJournal(s.path(sessionID), s.schemaID, through)
}

// Sessions returns the safe filename stems for every journal in the store.
func (s *Store) Sessions() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("journal: list sessions: %w", err)
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != journalFileExtension {
			continue
		}
		out = append(out, strings.TrimSuffix(entry.Name(), journalFileExtension))
	}
	return out, nil
}

// Close closes every open writer. The store can open writers again after Close.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var joined error
	for sessionID, writer := range s.writers {
		if err := writer.file.Close(); err != nil {
			joined = errors.Join(joined, fmt.Errorf("close %s: %w", sessionID, err))
		}
	}
	s.writers = map[string]*writer{}
	return joined
}

func (s *Store) writerLocked(sessionID string) (*writer, error) {
	if writer := s.writers[sessionID]; writer != nil {
		return writer, nil
	}

	events, err := readJournal(s.path(sessionID), s.schemaID, 0)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	file, err := os.OpenFile(s.path(sessionID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("journal: open %s: %w", sessionID, err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("journal: secure %s: %w", sessionID, err)
	}

	writer := &writer{file: file}
	if len(events) > 0 {
		writer.seq = events[len(events)-1].Sequence
	}
	if s.reducer != nil {
		for _, event := range events {
			nextState, _, reduceErr := s.reducer(writer.state, event)
			if reduceErr != nil {
				_ = file.Close()
				return nil, fmt.Errorf("journal: replay %s: %w", sessionID, reduceErr)
			}
			writer.state = cloneRawMessage(nextState)
		}
	}
	s.writers[sessionID] = writer
	return writer, nil
}

func (s *Store) path(sessionID string) string {
	return filepath.Join(s.dir, safeSessionID(sessionID)+journalFileExtension)
}

func readJournal(path, expectedSchema string, through uint64) ([]Record, error) {
	// #nosec G304 -- path is derived from the configured store directory and a sanitized session ID.
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)
	var out []Record
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if readErr == io.EOF && line[len(line)-1] != '\n' {
				break
			}
			var record Record
			if err := json.Unmarshal(line, &record); err != nil {
				return nil, fmt.Errorf("journal: decode %s line %d: %w", path, len(out)+1, err)
			}
			if record.Schema != expectedSchema || record.SchemaVersion != schemaVersion {
				return nil, fmt.Errorf("journal: unsupported schema at %s line %d", path, len(out)+1)
			}
			if len(out) > 0 && record.Sequence <= out[len(out)-1].Sequence {
				return nil, fmt.Errorf("journal: non-monotonic sequence at %s line %d", path, len(out)+1)
			}
			if through == 0 || record.Sequence <= through {
				out = append(out, record)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("journal: read %s: %w", path, readErr)
		}
	}
	return out, nil
}

func safeSessionID(sessionID string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, strings.TrimSpace(sessionID))
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func newEventID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("evt-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}
