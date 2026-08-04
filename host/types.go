package host

import (
	"context"
	"encoding/json"
)

type Backend string

type BackendStatus struct {
	Backend   Backend `json:"backend"`
	Available bool    `json:"available"`
	Command   string  `json:"command,omitempty"`
	Version   string  `json:"version,omitempty"`
	Error     string  `json:"error,omitempty"`
}

type BackendCatalog struct {
	Models          []BackendModel          `json:"models,omitempty"`
	PermissionModes []BackendPermissionMode `json:"permission_modes,omitempty"`
	Reasoning       []BackendReasoning      `json:"reasoning,omitempty"`
	SlashCommands   []BackendSlashCommand   `json:"slash_commands,omitempty"`
}

type BackendModel struct {
	ID        string             `json:"id"`
	Label     string             `json:"label,omitempty"`
	Reasoning []BackendReasoning `json:"reasoning"`
}

type BackendPermissionMode struct {
	ID          string `json:"id"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
}

type BackendReasoning struct {
	ID          string `json:"id"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
}

type BackendSlashCommand struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	InputHint   string `json:"input_hint,omitempty"`
}

type Attachment struct {
	ID         string `json:"id,omitempty"`
	Name       string `json:"name"`
	MimeType   string `json:"mime_type,omitempty"`
	Size       int64  `json:"size,omitempty"`
	DataBase64 string `json:"data_base64,omitempty"`
	Path       string `json:"path,omitempty"`
}

type BackendSession struct {
	ID       string `json:"id,omitempty"`
	ThreadID string `json:"thread_id,omitempty"`
	TurnID   string `json:"turn_id,omitempty"`
}

type SessionConfiguration struct {
	Model          string `json:"model,omitempty"`
	Reasoning      string `json:"reasoning,omitempty"`
	PermissionMode string `json:"permission_mode,omitempty"`
}

type EventType string

const (
	EventMessage              EventType = "message"
	EventThinking             EventType = "thinking"
	EventToolStarted          EventType = "tool_started"
	EventToolOutput           EventType = "tool_output"
	EventPermission           EventType = "permission_request"
	EventFileChanged          EventType = "file_changed"
	EventTurnStarted          EventType = "turn_started"
	EventTurnComplete         EventType = "turn_completed"
	EventTurnFailed           EventType = "turn_failed"
	EventProcessExited        EventType = "process_exited"
	EventQueueUpdated         EventType = "queue_updated"
	EventTraceUpdated         EventType = "trace_updated"
	EventAgentStalled         EventType = "agent_stalled"
	EventAgentRecovered       EventType = "agent_recovered"
	EventAgentResumeFailed    EventType = "agent_resume_failed"
	EventPlanUpdate           EventType = "plan_update"
	EventTodoUpdate           EventType = "todo_update"
	EventPermissionModes      EventType = "permission_modes"
	EventModels               EventType = "models"
	EventReasoningLevels      EventType = "reasoning_levels"
	EventAvailableCommands    EventType = "available_commands"
	EventConfigCatalog        EventType = "config_catalog"
	EventInteractionRequested EventType = "interaction_requested"
	EventInteractionResolved  EventType = "interaction_resolved"
)

type ToolDisplay struct {
	ID      string `json:"id,omitempty"`
	Title   string `json:"title"`
	Kind    string `json:"kind,omitempty"`
	Status  string `json:"status,omitempty"`
	Command string `json:"command,omitempty"`
	Target  string `json:"target,omitempty"`
}

// InteractionKind describes a request that needs an explicit user response.
// It intentionally describes UI-neutral concepts so a desktop app, terminal
// client, or service can render the same provider request in its own way.
type InteractionKind string

const (
	InteractionPermission InteractionKind = "permission"
	InteractionChoice     InteractionKind = "choice"
	InteractionForm       InteractionKind = "form"
	InteractionPlan       InteractionKind = "plan"
)

// InteractionOption is one selectable answer offered by an interaction.
type InteractionOption struct {
	ID          string `json:"id"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
}

// InteractionField describes one value in a form interaction.
type InteractionField struct {
	ID            string              `json:"id"`
	Label         string              `json:"label,omitempty"`
	Description   string              `json:"description,omitempty"`
	Required      bool                `json:"required,omitempty"`
	Options       []InteractionOption `json:"options,omitempty"`
	AllowFreeText bool                `json:"allow_free_text,omitempty"`
}

// InteractionRequest is emitted when an adapter is blocked on user input.
type InteractionRequest struct {
	ID      string              `json:"id"`
	Kind    InteractionKind     `json:"kind"`
	Title   string              `json:"title,omitempty"`
	Message string              `json:"message,omitempty"`
	Options []InteractionOption `json:"options,omitempty"`
	Fields  []InteractionField  `json:"fields,omitempty"`
	Data    map[string]any      `json:"data,omitempty"`
}

// InteractionResponse resolves an InteractionRequest. Action is one of the
// adapter-supported response actions (normally approve, deny, submit, or
// cancel); OptionID and Values carry the selected choice and form content.
type InteractionResponse struct {
	RequestID string         `json:"request_id"`
	Action    string         `json:"action"`
	OptionID  string         `json:"option_id,omitempty"`
	Message   string         `json:"message,omitempty"`
	Values    map[string]any `json:"values,omitempty"`
}

type Event struct {
	SourceEventID       string               `json:"source_event_id,omitempty"`
	SessionID           string               `json:"session_id"`
	Backend             Backend              `json:"backend,omitempty"`
	BackendSessionID    string               `json:"backend_session_id,omitempty"`
	BackendThreadID     string               `json:"backend_thread_id,omitempty"`
	BackendTurnID       string               `json:"backend_turn_id,omitempty"`
	Seq                 int                  `json:"seq,omitempty"`
	Type                EventType            `json:"type"`
	Role                string               `json:"role,omitempty"`
	Message             string               `json:"message,omitempty"`
	Data                map[string]any       `json:"data,omitempty"`
	ToolDisplay         *ToolDisplay         `json:"tool_display,omitempty"`
	Interaction         *InteractionRequest  `json:"interaction,omitempty"`
	InteractionResponse *InteractionResponse `json:"interaction_response,omitempty"`
	Time                string               `json:"time"`
	Local               map[string]any       `json:"-"`
}

type EventSink func(Event)

const (
	EventLocalReplay      = "durable-acp.replay"
	EventLocalReplayStart = "durable-acp.replay_start"
)

type StartSessionRequest struct {
	Backend   Backend         `json:"backend"`
	SessionID string          `json:"session_id,omitempty"`
	Worktree  string          `json:"worktree"`
	Prompt    string          `json:"prompt,omitempty"`
	Ext       json.RawMessage `json:"ext,omitempty"`

	Attachments []Attachment `json:"attachments,omitempty"`
	Model       string       `json:"model,omitempty"`
	Reasoning   string       `json:"reasoning,omitempty"`

	PermissionMode         string `json:"permission_mode,omitempty"`
	ResumeBackendSessionID string `json:"resume_backend_session_id,omitempty"`
}

type SendTurnRequest struct {
	SessionID      string          `json:"session_id"`
	Prompt         string          `json:"prompt"`
	Ext            json.RawMessage `json:"ext,omitempty"`
	Attachments    []Attachment    `json:"attachments,omitempty"`
	Model          string          `json:"model,omitempty"`
	Reasoning      string          `json:"reasoning,omitempty"`
	PermissionMode string          `json:"permission_mode,omitempty"`
}

type ForkPromptRequest struct {
	SessionID    string          `json:"session_id"`
	Prompt       string          `json:"prompt"`
	Instructions string          `json:"instructions,omitempty"`
	MCPServers   []ForkMCPServer `json:"mcp_servers,omitempty"`
}

type ForkPromptResponse struct {
	Accepted bool `json:"accepted"`
}

type ForkMCPServer struct {
	Type    string              `json:"type,omitempty"`
	Name    string              `json:"name"`
	Command string              `json:"command,omitempty"`
	Args    []string            `json:"args,omitempty"`
	Env     []ForkMCPEnv        `json:"env,omitempty"`
	URL     string              `json:"url,omitempty"`
	Headers []ForkMCPHTTPHeader `json:"headers,omitempty"`
}

type ForkMCPEnv struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ForkMCPHTTPHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Adapter emits an identified turn_started before turn-scoped, turn_completed,
// or turn_failed events. All events for that turn use the same non-empty
// BackendTurnID, which also matches the returned BackendSession.TurnID.
// Interrupt ends only the active turn and preserves the provider process and
// session for follow-up operations. CloseSession is the teardown boundary.
type Adapter interface {
	Backend() Backend
	Detect(ctx context.Context) BackendStatus
	StartSession(ctx context.Context, sessionID string, req StartSessionRequest, emit EventSink) (BackendSession, error)
	SendTurn(ctx context.Context, sessionID string, req SendTurnRequest, emit EventSink) (BackendSession, error)
	Interrupt(ctx context.Context, sessionID string, emit EventSink) error
	CloseSession(sessionID string) error
}

type SessionForker interface {
	ForkPrompt(ctx context.Context, req ForkPromptRequest) (ForkPromptResponse, error)
}

type SessionForkCapability interface {
	SessionForkSupport(sessionID string) (bool, string)
}

type AgentRestarter interface {
	RestartSession(ctx context.Context, sessionID string, emit EventSink) (BackendSession, error)
}

type PermissionResponder interface {
	RespondPermission(ctx context.Context, sessionID, requestID string, allow bool, message, permissionMode string) error
}

// InteractionResponder is implemented by adapters that can resolve general
// permission, choice, form, or plan interactions.
type InteractionResponder interface {
	RespondInteraction(ctx context.Context, sessionID string, response InteractionResponse) error
}

func AdapterSessionForkSupport(adapter Adapter, sessionID string) (bool, string) {
	return adapterSessionForkSupport(adapter, sessionID)
}

func adapterSessionForkSupport(adapter Adapter, sessionID string) (bool, string) {
	if _, ok := adapter.(SessionForker); !ok {
		return false, ""
	}
	if capability, ok := adapter.(SessionForkCapability); ok {
		return capability.SessionForkSupport(sessionID)
	}
	return true, ""
}
