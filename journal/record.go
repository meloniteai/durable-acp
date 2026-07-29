package journal

import (
	"encoding/json"
	"time"
)

// Presentation contains optional display hints for a journal record.
//
// The journal stores these hints but does not interpret them.
type Presentation struct {
	Surfaces  []string `json:"surfaces,omitempty"`
	Label     string   `json:"label,omitempty"`
	Detail    string   `json:"detail,omitempty"`
	Attention bool     `json:"attention,omitempty"`
}

// Record is one durable journal entry.
//
// Data is application-defined JSON. Conversation and Event are intentionally
// unconstrained by the store so hosts can define their own vocabularies.
type Record struct {
	Schema        string          `json:"schema"`
	SchemaVersion int             `json:"schema_version"`
	EventVersion  int             `json:"event_version"`
	EventID       string          `json:"event_id"`
	Sequence      uint64          `json:"sequence"`
	Timestamp     time.Time       `json:"timestamp"`
	SessionID     string          `json:"session_id"`
	Conversation  string          `json:"conversation"`
	Event         string          `json:"event"`
	TurnID        string          `json:"turn_id,omitempty"`
	Data          json.RawMessage `json:"data"`
	Presentation  *Presentation   `json:"presentation,omitempty"`
}
