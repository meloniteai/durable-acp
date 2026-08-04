package runtime

import (
	"sync"
	"testing"
	"time"

	"github.com/meloniteai/durable-acp/host"
)

func TestDeltaCoalescerSnapshotsAndClose(t *testing.T) {
	var mu sync.Mutex
	var got []host.Event
	coalescer := newDeltaCoalescer(time.Hour, func(event host.Event) {
		mu.Lock()
		got = append(got, event)
		mu.Unlock()
	})
	coalescer.Handle(host.Event{SessionID: "s", BackendTurnID: "t", Type: host.EventMessage, Message: "a", Data: map[string]any{"delta": "a", "provider_event_id": "provider-1"}})
	coalescer.Handle(host.Event{SessionID: "s", BackendTurnID: "t", Type: host.EventMessage, Message: "b", Data: map[string]any{"delta": "b"}})
	coalescer.Handle(host.Event{SessionID: "s", BackendTurnID: "t", Type: host.EventMessage, Message: "ab"})
	coalescer.Close()
	coalescer.Handle(host.Event{Type: host.EventMessage, Message: "ignored"})

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("events = %#v", got)
	}
	if got[0].Message != "a" || got[1].Message != "ab" || got[1].SourceEventID != got[0].SourceEventID {
		t.Fatalf("snapshots = %#v", got)
	}
	if got[0].Data["provider_event_id"] != "provider-1" || got[0].Data["streaming"] != true {
		t.Fatalf("stream metadata = %#v", got[0].Data)
	}
}

func TestDeltaCoalescerFlushesOnBoundaryAndTimer(t *testing.T) {
	events := make(chan host.Event, 8)
	coalescer := newDeltaCoalescer(5*time.Millisecond, func(event host.Event) { events <- event })
	coalescer.Handle(host.Event{SessionID: "s", BackendTurnID: "one", Type: host.EventThinking, Message: "a", Data: map[string]any{"delta": "a"}})
	first := <-events
	if first.Message != "a" || first.Type != host.EventThinking {
		t.Fatalf("initial event = %#v", first)
	}
	coalescer.Handle(host.Event{SessionID: "s", BackendTurnID: "one", Type: host.EventThinking, Message: "b", Data: map[string]any{"delta": "b"}})
	select {
	case event := <-events:
		if event.Message != "ab" || event.Type != host.EventThinking {
			t.Fatalf("timer event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timer did not flush a delta")
	}
	coalescer.Handle(host.Event{SessionID: "s", BackendTurnID: "two", Type: host.EventMessage, Message: "next", Data: map[string]any{"delta": "next"}})
	coalescer.Handle(host.Event{Type: host.EventToolStarted})
	boundary := <-events
	second := <-events
	if boundary.Message != "next" || second.Type != host.EventToolStarted {
		t.Fatalf("boundary events = %#v, %#v", boundary, second)
	}
}

func TestDeltaCoalescerHelpers(t *testing.T) {
	if !isAssistantDelta(host.Event{Type: host.EventMessage, Data: map[string]any{"delta": "x"}}) {
		t.Fatal("assistant delta not recognized")
	}
	if isAssistantDelta(host.Event{Type: host.EventMessage, Role: "user", Data: map[string]any{"delta": "x"}}) {
		t.Fatal("user delta recognized")
	}
	if !isAssistantSnapshot(host.Event{Type: host.EventThinking, Data: map[string]any{}}) {
		t.Fatal("assistant snapshot not recognized")
	}
	if isAssistantSnapshot(host.Event{Type: host.EventThinking, Role: "user"}) {
		t.Fatal("user snapshot recognized")
	}
	if got := streamSourceID("s", "", 2); got != "s:turn:-:msg:2" {
		t.Fatalf("stream ID = %q", got)
	}
	if got := streamData(host.Event{}); got["streaming"] != true || len(got) != 1 {
		t.Fatalf("stream data = %#v", got)
	}
}

func TestDeltaCoalescerPreservesBlankLines(t *testing.T) {
	chunks := []string{"**What's actually broken**", "\n\n", "| Layer | Bug |"}
	coalescer := newDeltaCoalescer(time.Hour, func(host.Event) {})
	for _, chunk := range chunks {
		coalescer.Handle(host.Event{Type: host.EventMessage, Message: chunk, BackendTurnID: "t1", Data: map[string]any{"delta": chunk}})
	}
	want := "**What's actually broken**\n\n| Layer | Bug |"
	if coalescer.pending == nil || coalescer.pending.Message != want {
		t.Fatalf("coalesced message = %#v, want %q", coalescer.pending, want)
	}
}
