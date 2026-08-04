package runtime

import (
	"fmt"
	"sync"
	"time"

	"github.com/meloniteai/durable-acp/host"
)

// deltaCoalescer turns rapid assistant token deltas into periodic cumulative
// snapshots. Every snapshot for one logical stream uses a stable source ID so
// frontends and journal readers can replace it in place.
type deltaCoalescer struct {
	mu       sync.Mutex
	deliver  func(host.Event)
	interval time.Duration

	turnID     string
	streamType host.EventType
	message    int
	pending    *host.Event
	flushed    int
	timer      *time.Timer
	closed     bool
}

func newDeltaCoalescer(interval time.Duration, deliver func(host.Event)) *deltaCoalescer {
	if interval <= 0 {
		interval = defaultCoalesceInterval
	}
	return &deltaCoalescer{deliver: deliver, interval: interval}
}

func (c *deltaCoalescer) Handle(event host.Event) {
	var events []host.Event
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	switch {
	case isAssistantDelta(event):
		if c.pending == nil || c.turnID != event.BackendTurnID || c.streamType != event.Type {
			if snapshot := c.flushLocked(); snapshot != nil {
				events = append(events, *snapshot)
			}
			c.clearLocked()
			c.message++
			snapshot := event
			snapshot.SourceEventID = streamSourceID(event.SessionID, event.BackendTurnID, c.message)
			snapshot.Data = streamData(event)
			c.turnID = event.BackendTurnID
			c.streamType = event.Type
			c.pending = &snapshot
			c.flushed = 0
			if snapshot := c.flushLocked(); snapshot != nil {
				events = append(events, *snapshot)
			}
		} else {
			c.pending.Message += event.Message
			c.pending.Time = event.Time
			c.scheduleLocked()
		}
	case c.pending != nil && isAssistantSnapshot(event) && event.BackendTurnID == c.turnID && event.Type == c.streamType:
		final := event
		if len(final.Message) < len(c.pending.Message) {
			final.Message = c.pending.Message
		}
		final.Seq = c.pending.Seq
		final.SourceEventID = c.pending.SourceEventID
		c.clearLocked()
		events = append(events, final)
	default:
		if snapshot := c.flushLocked(); snapshot != nil {
			events = append(events, *snapshot)
		}
		c.clearLocked()
		events = append(events, event)
	}
	c.mu.Unlock()
	c.emit(events)
}

func (c *deltaCoalescer) Close() {
	var events []host.Event
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if snapshot := c.flushLocked(); snapshot != nil {
		events = append(events, *snapshot)
	}
	c.closed = true
	c.clearLocked()
	c.mu.Unlock()
	c.emit(events)
}

func (c *deltaCoalescer) scheduleLocked() {
	if c.timer != nil {
		return
	}
	c.timer = time.AfterFunc(c.interval, func() {
		c.mu.Lock()
		c.timer = nil
		var snapshot *host.Event
		if !c.closed {
			snapshot = c.flushLocked()
		}
		c.mu.Unlock()
		if snapshot != nil {
			c.emit([]host.Event{*snapshot})
		}
	})
}

func (c *deltaCoalescer) flushLocked() *host.Event {
	if c.pending == nil || len(c.pending.Message) == c.flushed {
		return nil
	}
	snapshot := *c.pending
	snapshot.Data = streamData(snapshot)
	c.flushed = len(snapshot.Message)
	return &snapshot
}

func (c *deltaCoalescer) clearLocked() {
	c.pending = nil
	c.flushed = 0
	c.turnID = ""
	c.streamType = ""
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

func (c *deltaCoalescer) emit(events []host.Event) {
	if c.deliver == nil {
		return
	}
	for _, event := range events {
		c.deliver(event)
	}
}

func isAssistantDelta(event host.Event) bool {
	if (event.Type != host.EventMessage && event.Type != host.EventThinking) || event.Role == "user" {
		return false
	}
	delta, _ := event.Data["delta"].(string)
	return delta != ""
}

func isAssistantSnapshot(event host.Event) bool {
	if (event.Type != host.EventMessage && event.Type != host.EventThinking) || event.Role == "user" {
		return false
	}
	delta, _ := event.Data["delta"].(string)
	return delta == ""
}

func streamSourceID(sessionID, turnID string, index int) string {
	if turnID == "" {
		turnID = "-"
	}
	return fmt.Sprintf("%s:turn:%s:msg:%d", sessionID, turnID, index)
}

func streamData(event host.Event) map[string]any {
	data := map[string]any{"streaming": true}
	if event.Data != nil {
		if sourceID, ok := event.Data["provider_event_id"].(string); ok && sourceID != "" {
			data["provider_event_id"] = sourceID
		}
	}
	return data
}
