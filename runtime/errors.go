package runtime

import "fmt"

type SessionNotFoundError struct {
	SessionID string
}

func (e *SessionNotFoundError) Error() string {
	return fmt.Sprintf("runtime: session %q not found", e.SessionID)
}

type SessionExistsError struct {
	SessionID string
}

func (e *SessionExistsError) Error() string {
	return fmt.Sprintf("runtime: session %q already exists", e.SessionID)
}

type SessionClosedError struct {
	SessionID string
}

func (e *SessionClosedError) Error() string {
	return fmt.Sprintf("runtime: session %q is closed", e.SessionID)
}

type QueueFullError struct {
	SessionID string
	Depth     int
	Limit     int
}

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("runtime: session %q turn queue full", e.SessionID)
}

type QueueEntryNotFoundError struct {
	SessionID string
	EntryID   string
}

func (e *QueueEntryNotFoundError) Error() string {
	return fmt.Sprintf("runtime: queue entry %q not found for session %q", e.EntryID, e.SessionID)
}

type TurnActiveError struct {
	SessionID string
	TurnID    string
}

func (e *TurnActiveError) Error() string {
	if e.TurnID == "" {
		return fmt.Sprintf("runtime: cannot restart session %q while a turn is active", e.SessionID)
	}
	return fmt.Sprintf("runtime: cannot restart session %q while turn %q is active", e.SessionID, e.TurnID)
}

type RestartUnsupportedError struct {
	Backend string
}

func (e *RestartUnsupportedError) Error() string {
	return fmt.Sprintf("runtime: backend %q does not support session restart", e.Backend)
}
