package journal

import (
	"encoding/json"
	"maps"
	"strings"

	"github.com/meloniteai/durable-acp/host"
)

const (
	EventUserMessage         = "user.message"
	EventUserRequestResolved = "user.request_resolved"
	EventAgentMessage        = "agent.message"
	EventAgentPermission     = "agent.permission_requested"
	EventAgentInteraction    = "agent.interaction_requested"
	EventAgentWorkspace      = "agent.workspace_changed"
	EventAgentTurnStarted    = "agent.turn_started"
	EventAgentYielded        = "agent.yielded"
	EventAgentInterrupted    = "agent.interrupted"
	EventAgentTurnFailed     = "agent.turn_failed"
	EventAgentProcessExited  = "agent.process_exited"
	EventAgentStalled        = "agent.stalled"
	EventAgentRecovered      = "agent.recovered"
	EventAgentResumeFailed   = "agent.resume_failed"
	EventAgentPlanProposed   = "agent.plan_proposed"
	EventAgentTodoUpdated    = "agent.todo_updated"
)

// Translate converts a normalized host event to the neutral journal
// vocabulary. Dotted host event types are preserved as host-owned extension
// names; unknown unqualified event types are not journaled.
func Translate(event host.Event) (Record, bool) {
	name := translatedEventName(event)
	if name == "" {
		return Record{}, false
	}

	data := make(map[string]any, len(event.Data)+2)
	maps.Copy(data, event.Data)
	if event.Message != "" {
		data["message"] = event.Message
	}
	if event.Interaction != nil {
		data["interaction"] = event.Interaction
	}
	if event.InteractionResponse != nil {
		data["interaction_response"] = event.InteractionResponse
	}
	if event.Backend != "" {
		data["agent"] = map[string]any{
			"backend":            event.Backend,
			"backend_session_id": event.BackendSessionID,
			"backend_thread_id":  event.BackendThreadID,
			"backend_turn_id":    event.BackendTurnID,
		}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return Record{}, false
	}
	return Record{
		SessionID: event.SessionID,
		Event:     name,
		TurnID:    event.BackendTurnID,
		Data:      raw,
	}, true
}

func translatedEventName(event host.Event) string {
	name := strings.TrimSpace(string(event.Type))
	if strings.Contains(name, ".") {
		return name
	}
	switch event.Type {
	case host.EventMessage:
		switch strings.ToLower(strings.TrimSpace(event.Role)) {
		case "user":
			return EventUserMessage
		case "assistant", "agent":
			return EventAgentMessage
		default:
			return ""
		}
	case host.EventPermission:
		return EventAgentPermission
	case host.EventInteractionRequested:
		if event.Interaction != nil && event.Interaction.Kind == host.InteractionPermission {
			return EventAgentPermission
		}
		return EventAgentInteraction
	case host.EventInteractionResolved:
		return EventUserRequestResolved
	case host.EventFileChanged:
		return EventAgentWorkspace
	case host.EventTurnStarted:
		return EventAgentTurnStarted
	case host.EventTurnComplete:
		return EventAgentYielded
	case host.EventTurnFailed:
		return EventAgentTurnFailed
	case host.EventProcessExited:
		return EventAgentProcessExited
	case host.EventAgentStalled:
		return EventAgentStalled
	case host.EventAgentRecovered:
		return EventAgentRecovered
	case host.EventAgentResumeFailed:
		return EventAgentResumeFailed
	case host.EventPlanUpdate:
		return EventAgentPlanProposed
	case host.EventTodoUpdate:
		return EventAgentTodoUpdated
	case host.EventThinking,
		host.EventToolStarted,
		host.EventToolOutput,
		host.EventQueueUpdated,
		host.EventTraceUpdated,
		host.EventPermissionModes,
		host.EventModels,
		host.EventReasoningLevels,
		host.EventAvailableCommands,
		host.EventConfigCatalog:
		return ""
	default:
		return ""
	}
}
