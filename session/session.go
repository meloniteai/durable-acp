package session

import (
	"errors"
	"fmt"
	"time"

	"github.com/meloniteai/durable-acp/host"
)

type Status string

const (
	StatusCreating     Status = "creating"
	StatusActive       Status = "active"
	StatusRunning      Status = "running"
	StatusWaitingInput Status = "waiting_input"
	StatusFailed       Status = "failed"
	StatusClosed       Status = "closed"
)

var ErrInvalidTransition = errors.New("invalid session status transition")

type Session struct {
	ID             string              `json:"id"`
	Backend        host.Backend        `json:"backend"`
	Worktree       string              `json:"worktree"`
	Status         Status              `json:"status"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
	BackendSession host.BackendSession `json:"backend_session,omitzero"`
}

var allowedTransitions = map[Status]map[Status]struct{}{
	StatusCreating: {
		StatusActive: {},
		StatusFailed: {},
		StatusClosed: {},
	},
	StatusActive: {
		StatusRunning: {},
		StatusFailed:  {},
		StatusClosed:  {},
	},
	StatusRunning: {
		StatusWaitingInput: {},
		StatusFailed:       {},
		StatusClosed:       {},
	},
	StatusWaitingInput: {
		StatusRunning: {},
		StatusFailed:  {},
		StatusClosed:  {},
	},
	StatusFailed: {
		StatusClosed: {},
	},
	StatusClosed: {},
}

func New(id string, backend host.Backend, worktree string) *Session {
	now := time.Now().UTC()
	return &Session{
		ID:        id,
		Backend:   backend,
		Worktree:  worktree,
		Status:    StatusCreating,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (s *Session) Transition(next Status) error {
	if s == nil {
		return fmt.Errorf("%w: nil session", ErrInvalidTransition)
	}
	if _, ok := allowedTransitions[s.Status][next]; !ok {
		return fmt.Errorf("%w: %q to %q", ErrInvalidTransition, s.Status, next)
	}
	s.Status = next
	s.UpdatedAt = time.Now().UTC()
	return nil
}
