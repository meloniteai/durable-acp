package acpx

import (
	"fmt"
	"strings"

	"github.com/meloniteai/durable-acp/host"
)

// ToolDisplayFromACP normalizes an untyped ACP tool-call update into the host
// display vocabulary. It accepts both direct and wrapped session/update forms.
func ToolDisplayFromACP(data map[string]any, status string) *host.ToolDisplay {
	source := data
	if update := mapValue(data, "update"); len(update) > 0 {
		source = update
	}
	toolCall := mapValue(source, "toolCall")
	if len(toolCall) == 0 {
		toolCall = mapValue(source, "toolCallUpdate")
	}
	if len(toolCall) == 0 {
		toolCall = source
	}
	title := toolDisplayTitle(toolCall)
	if title == "" {
		title = toolDisplayTitle(source)
	}
	kind := NormalizeToolKind(stringAtValue(toolCall, "kind"))
	if kind == "" {
		kind = NormalizeToolKind(stringAtValue(source, "kind"))
	}
	command := toolDisplayCommand(toolCall)
	target := ToolDisplayTarget(toolCall)
	if target == "" {
		target = ToolDisplayTarget(source)
	}
	if title == "" {
		title = DefaultToolTitle(kind, command)
	}
	if title == "" {
		if modeID := toolModeTargetID(toolCall); modeID != "" && strings.EqualFold(strings.TrimSpace(stringValue(toolCall, "kind")), "switch_mode") {
			title = switchModeTitle(modeID)
		}
	}
	if title == "" {
		return nil
	}
	display := &host.ToolDisplay{
		ID:      FirstNonEmpty(firstString(toolCall, "toolCallId", "tool_call_id", "id"), firstString(source, "toolCallId", "tool_call_id", "id")),
		Title:   title,
		Kind:    kind,
		Status:  NormalizeToolStatus(FirstNonEmpty(status, stringAtValue(toolCall, "status"), stringAtValue(source, "status"))),
		Command: command,
		Target:  target,
	}
	polishSwitchModeDisplay(toolCall, display)
	return display
}

// CommandToolDisplay returns a conventional display record for a shell command.
func CommandToolDisplay(command, status string) *host.ToolDisplay {
	command = strings.TrimSpace(command)
	return &host.ToolDisplay{
		Title:   DefaultToolTitle("execute", command),
		Kind:    "execute",
		Status:  NormalizeToolStatus(status),
		Command: command,
		Target:  command,
	}
}

// ToolDisplayFromCodexItem maps Codex's native item envelope to host display
// data without retaining any provider-specific state.
func ToolDisplayFromCodexItem(item map[string]any, status string) *host.ToolDisplay {
	if item == nil {
		return nil
	}
	switch strings.TrimSpace(stringValue(item, "type")) {
	case "commandExecution":
		return CommandToolDisplay(CodexCommandFromData(item), status)
	case "fileChange":
		return &host.ToolDisplay{
			Title:  DefaultToolTitle("edit", ""),
			Kind:   "edit",
			Status: NormalizeToolStatus(status),
			Target: ToolDisplayTarget(item),
		}
	case "mcpToolCall", "dynamicToolCall":
		title := toolDisplayTitle(item)
		if title == "" {
			title = "tool call"
		}
		return &host.ToolDisplay{
			ID:      firstString(item, "id", "itemId", "callId", "toolCallId"),
			Title:   title,
			Kind:    NormalizeToolKind(FirstNonEmpty(stringValue(item, "kind"), stringValue(item, "type"))),
			Status:  NormalizeToolStatus(status),
			Command: toolDisplayCommand(item),
			Target:  ToolDisplayTarget(item),
		}
	default:
		return nil
	}
}

// CodexCommandFromData extracts a command from Codex's direct or nested item
// envelope.
func CodexCommandFromData(data map[string]any) string {
	if command := stringValue(data, "command"); command != "" {
		return strings.TrimSpace(command)
	}
	if item := mapValue(data, "item"); item != nil {
		if command := stringValue(item, "command"); command != "" {
			return strings.TrimSpace(command)
		}
	}
	return ""
}

// PermissionToolDisplay builds a stable display record for an ACP permission.
func PermissionToolDisplay(toolName string, input map[string]any, status string) *host.ToolDisplay {
	command := toolDisplayCommand(input)
	kind := NormalizeToolKind(stringValue(input, "kind"))
	if kind == "" && command != "" {
		kind = "execute"
	}
	title := strings.TrimSpace(toolName)
	if title == "" {
		title = toolDisplayTitle(input)
	}
	if title == "" {
		title = DefaultToolTitle(kind, command)
	}
	if title == "" {
		title = "permission requested"
	}
	return &host.ToolDisplay{Title: title, Kind: kind, Status: NormalizeToolStatus(status), Command: command, Target: ToolDisplayTarget(input)}
}

// ToolDisplayTarget finds a human-useful command target or file target inside
// direct and nested ACP payloads.
func ToolDisplayTarget(data map[string]any) string {
	if data == nil {
		return ""
	}
	if target := directToolDisplayTarget(data); target != "" {
		return target
	}
	if target := fileChangesTarget(data); target != "" {
		return target
	}
	for _, key := range []string{"args", "arguments", "input", "rawInput"} {
		if nested := mapValue(data, key); nested != nil {
			if target := directToolDisplayTarget(nested); target != "" {
				return target
			}
			if target := fileChangesTarget(nested); target != "" {
				return target
			}
		}
	}
	if locations := anyValues(data["locations"]); len(locations) > 0 {
		if location, _ := locations[0].(map[string]any); location != nil {
			return strings.TrimSpace(firstString(location, "path", "uri", "url"))
		}
	}
	return ""
}

// ToolDisplayCommand finds a shell command in direct or nested ACP payloads.
func ToolDisplayCommand(data map[string]any) string {
	return toolDisplayCommand(data)
}

// NormalizeToolKind maps common ACP and provider labels to the host's compact
// tool-kind vocabulary.
func NormalizeToolKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "read", "edit", "delete", "move", "search", "execute", "think", "fetch", "other":
		return strings.ToLower(strings.TrimSpace(kind))
	case "switch_mode", "switchmode":
		return "other"
	case "shell", "bash", "command", "commandexecution", "command execution":
		return "execute"
	case "filechange", "file change", "write":
		return "edit"
	default:
		return ""
	}
}

// NormalizeToolStatus maps common ACP and provider labels to host tool states.
func NormalizeToolStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending":
		return "pending"
	case "in_progress", "in-progress", "running", "started":
		return "in_progress"
	case "completed", "complete", "done", "ok", "success", "allow", "allowed", "approved":
		return "completed"
	case "failed", "failure", "error", "cancelled", "canceled", "denied", "deny", "rejected":
		return "failed"
	default:
		return ""
	}
}

// DefaultToolTitle returns a concise fallback title for a normalized kind.
func DefaultToolTitle(kind, command string) string {
	switch kind {
	case "execute":
		if command != "" {
			return command
		}
		return "command"
	case "read":
		return "read"
	case "edit":
		return "edit"
	case "search":
		return "search"
	case "fetch":
		return "fetch"
	case "think":
		return "thinking"
	default:
		return ""
	}
}

// FirstNonEmpty returns the first non-empty trimmed string.
func FirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// ToolCallIDFromData extracts a tool call ID from direct and wrapped ACP data.
func ToolCallIDFromData(data map[string]any) string {
	if len(data) == 0 {
		return ""
	}
	source := data
	if update := mapValue(data, "update"); len(update) > 0 {
		source = update
	}
	return FirstNonEmpty(
		firstString(source, "toolCallId", "tool_call_id"),
		firstString(mapValue(source, "toolCall"), "toolCallId", "tool_call_id", "id"),
		firstString(mapValue(source, "toolCallUpdate"), "toolCallId", "tool_call_id", "id"),
	)
}

func toolDisplayTitle(data map[string]any) string {
	if data == nil {
		return ""
	}
	if title := stringValue(data, "title"); title != "" {
		return strings.TrimSpace(title)
	}
	if title := mapValue(data, "title"); title != nil {
		return strings.TrimSpace(firstString(title, "name", "toolName", "tool_name"))
	}
	return strings.TrimSpace(firstString(data, "name", "toolName", "tool_name"))
}

func toolDisplayCommand(data map[string]any) string {
	if data == nil {
		return ""
	}
	if command := stringValue(data, "command"); command != "" {
		return strings.TrimSpace(command)
	}
	for _, key := range []string{"args", "arguments", "input", "rawInput"} {
		if nested := mapValue(data, key); nested != nil {
			if command := stringValue(nested, "command"); command != "" {
				return strings.TrimSpace(command)
			}
		}
	}
	return ""
}

func directToolDisplayTarget(data map[string]any) string {
	for _, key := range []string{"target", "path", "file", "filePath", "file_path", "url", "uri", "query", "pattern"} {
		if value := stringValue(data, key); value != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func fileChangesTarget(data map[string]any) string {
	if data == nil {
		return ""
	}
	for _, key := range []string{"fileChanges", "changes"} {
		raw, ok := data[key].(map[string]any)
		if !ok || len(raw) == 0 {
			continue
		}
		paths := make([]string, 0, len(raw))
		for path := range raw {
			if trimmed := strings.TrimSpace(path); trimmed != "" {
				paths = append(paths, trimmed)
			}
		}
		if len(paths) == 1 {
			return paths[0]
		}
		if len(paths) > 1 {
			return fmt.Sprintf("%d files", len(paths))
		}
	}
	return ""
}

func firstString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(stringValue(data, key)); value != "" {
			return value
		}
	}
	return ""
}

func polishSwitchModeDisplay(toolCall map[string]any, display *host.ToolDisplay) {
	if display == nil {
		return
	}
	modeID := toolModeTargetID(toolCall)
	if modeID == "" || !isSwitchModeDisplay(display.Title, toolCall) {
		return
	}
	display.Title = switchModeTitle(modeID)
	if display.Kind == "" {
		display.Kind = "other"
	}
}

func isSwitchModeDisplay(title string, toolCall map[string]any) bool {
	if strings.EqualFold(strings.TrimSpace(stringValue(toolCall, "kind")), "switch_mode") {
		return true
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(title)), "switch mode")
}

func switchModeTitle(modeID string) string {
	label := humanModeLabel(modeID)
	if label == "" {
		return "Switch mode"
	}
	return "Switch to " + label + " mode"
}

func humanModeLabel(modeID string) string {
	switch strings.ToLower(strings.TrimSpace(modeID)) {
	case "plan":
		return "plan"
	case "agent", "default":
		return "agent"
	case "ask":
		return "ask"
	case "debug":
		return "debug"
	default:
		return strings.TrimSpace(modeID)
	}
}

func toolModeTargetID(data map[string]any) string {
	if len(data) == 0 {
		return ""
	}
	if id := firstString(data, "target_mode_id", "targetModeId", "modeId", "mode_id"); id != "" {
		return id
	}
	for _, key := range []string{"args", "arguments", "input", "rawInput"} {
		if nested := mapValue(data, key); nested != nil {
			if id := toolModeTargetID(nested); id != "" {
				return id
			}
		}
	}
	return ""
}

func mapValue(data map[string]any, key string) map[string]any {
	value, _ := data[key].(map[string]any)
	if value == nil {
		return map[string]any{}
	}
	return value
}

func stringValue(data map[string]any, key string) string {
	value, _ := data[key].(string)
	return value
}

func stringAtValue(data map[string]any, path ...string) string {
	var value any = data
	for _, key := range path {
		current, ok := value.(map[string]any)
		if !ok {
			return ""
		}
		value = current[key]
	}
	text, _ := value.(string)
	return text
}

func anyValues(raw any) []any {
	switch values := raw.(type) {
	case []any:
		return values
	case []map[string]any:
		result := make([]any, len(values))
		for index := range values {
			result[index] = values[index]
		}
		return result
	default:
		return nil
	}
}
