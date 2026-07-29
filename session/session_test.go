package session

import (
	"errors"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/host"
)

func TestNew(t *testing.T) {
	t.Parallel()

	created := New("session-1", host.Backend("test"), "/worktree")
	if created.ID != "session-1" || created.Backend != host.Backend("test") || created.Worktree != "/worktree" {
		t.Fatalf("session identity = %+v", created)
	}
	if created.Status != StatusCreating {
		t.Fatalf("status = %q, want %q", created.Status, StatusCreating)
	}
	if created.CreatedAt.IsZero() || !created.CreatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("timestamps = (%s, %s), want equal non-zero values", created.CreatedAt, created.UpdatedAt)
	}
}

func TestTransitionTable(t *testing.T) {
	t.Parallel()

	statuses := []Status{
		StatusCreating,
		StatusActive,
		StatusRunning,
		StatusWaitingInput,
		StatusFailed,
		StatusClosed,
	}
	allowed := map[Status]map[Status]bool{
		StatusCreating:     {StatusActive: true, StatusFailed: true, StatusClosed: true},
		StatusActive:       {StatusRunning: true, StatusFailed: true, StatusClosed: true},
		StatusRunning:      {StatusActive: true, StatusWaitingInput: true, StatusFailed: true, StatusClosed: true},
		StatusWaitingInput: {StatusActive: true, StatusRunning: true, StatusFailed: true, StatusClosed: true},
		StatusFailed:       {StatusClosed: true},
		StatusClosed:       {},
	}

	for _, from := range statuses {
		for _, to := range statuses {
			from, to := from, to
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				t.Parallel()

				before := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
				session := &Session{Status: from, UpdatedAt: before}
				err := session.Transition(to)
				if allowed[from][to] {
					if err != nil {
						t.Fatalf("Transition(%q, %q): %v", from, to, err)
					}
					if session.Status != to || !session.UpdatedAt.After(before) {
						t.Fatalf("session = %+v, want status %q and a newer timestamp", session, to)
					}
					return
				}
				if !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("Transition(%q, %q) error = %v, want ErrInvalidTransition", from, to, err)
				}
				if session.Status != from || !session.UpdatedAt.Equal(before) {
					t.Fatalf("invalid transition mutated session: %+v", session)
				}
			})
		}
	}
}

func TestTransitionRejectsUnknownAndNilSessions(t *testing.T) {
	t.Parallel()

	unknown := &Session{Status: Status("unknown")}
	if err := unknown.Transition(StatusActive); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown status error = %v, want ErrInvalidTransition", err)
	}
	var nilSession *Session
	if err := nilSession.Transition(StatusActive); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("nil session error = %v, want ErrInvalidTransition", err)
	}
}
