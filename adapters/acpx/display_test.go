package acpx

import "testing"

func TestToolDisplayFromACPSwitchMode(t *testing.T) {
	display := ToolDisplayFromACP(map[string]any{
		"update": map[string]any{
			"toolCall": map[string]any{
				"toolCallId": "call-switch-1",
				"title":      "Switch Mode: unknown",
				"kind":       "switch_mode",
				"rawInput":   map[string]any{"target_mode_id": "plan"},
			},
		},
	}, "in_progress")
	if display == nil || display.Title != "Switch to plan mode" || display.Kind != "other" {
		t.Fatalf("display = %#v", display)
	}
}

func TestToolDisplayFromCodexFileChange(t *testing.T) {
	display := ToolDisplayFromCodexItem(map[string]any{
		"type": "fileChange",
		"fileChanges": map[string]any{
			"a.ts": map[string]any{},
			"b.ts": map[string]any{},
		},
	}, "completed")
	if display == nil || display.Kind != "edit" || display.Target != "2 files" || display.Status != "completed" {
		t.Fatalf("display = %#v", display)
	}
}

func TestPermissionToolDisplayAndToolCallID(t *testing.T) {
	display := PermissionToolDisplay("", map[string]any{"input": map[string]any{"command": "go test ./..."}}, "allow")
	if display == nil || display.Title != "go test ./..." || display.Kind != "execute" || display.Status != "completed" {
		t.Fatalf("display = %#v", display)
	}
	id := ToolCallIDFromData(map[string]any{"update": map[string]any{"toolCallUpdate": map[string]any{"id": "tool-1"}}})
	if id != "tool-1" {
		t.Fatalf("tool call ID = %q", id)
	}
}

func TestToolDisplayNormalizersCoverProviderForms(t *testing.T) {
	command := CommandToolDisplay("go test ./...", "running")
	if command.Title != "go test ./..." || command.Kind != "execute" || command.Status != "in_progress" {
		t.Fatalf("command = %#v", command)
	}
	for input, want := range map[string]string{
		"shell": "execute", "write": "edit", "switch_mode": "other", "bogus": "",
	} {
		if got := NormalizeToolKind(input); got != want {
			t.Fatalf("kind %q = %q, want %q", input, got, want)
		}
	}
	for input, want := range map[string]string{
		"allow": "completed", "cancelled": "failed", "unknown": "",
	} {
		if got := NormalizeToolStatus(input); got != want {
			t.Fatalf("status %q = %q, want %q", input, got, want)
		}
	}
	if got := DefaultToolTitle("execute", ""); got != "command" {
		t.Fatalf("default title = %q", got)
	}
	if got := FirstNonEmpty("", " first ", "last"); got != " first " {
		t.Fatalf("first non-empty = %q", got)
	}
	if got := CodexCommandFromData(map[string]any{"item": map[string]any{"command": "npm test"}}); got != "npm test" {
		t.Fatalf("nested command = %q", got)
	}
}

func TestToolDisplayTargetsAndCodexItems(t *testing.T) {
	for _, test := range []struct {
		name string
		data map[string]any
		want string
	}{
		{name: "nested", data: map[string]any{"input": map[string]any{"path": "src/main.go"}}, want: "src/main.go"},
		{name: "single change", data: map[string]any{"changes": map[string]any{"README.md": map[string]any{}}}, want: "README.md"},
		{name: "location", data: map[string]any{"locations": []any{map[string]any{"uri": "file:///tmp/a"}}}, want: "file:///tmp/a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ToolDisplayTarget(test.data); got != test.want {
				t.Fatalf("target = %q, want %q", got, test.want)
			}
		})
	}
	mcp := ToolDisplayFromCodexItem(map[string]any{"type": "mcpToolCall", "id": "call", "name": "lookup", "input": map[string]any{"query": "agents"}}, "pending")
	if mcp == nil || mcp.ID != "call" || mcp.Title != "tool call" || mcp.Target != "agents" {
		t.Fatalf("mcp display = %#v", mcp)
	}
	if got := ToolDisplayFromCodexItem(map[string]any{"type": "unknown"}, "completed"); got != nil {
		t.Fatalf("unknown display = %#v", got)
	}
}

func TestToolDisplayFromACPFallbacksAndHelpers(t *testing.T) {
	display := ToolDisplayFromACP(map[string]any{
		"toolCallUpdate": map[string]any{
			"tool_call_id": "tool-2",
			"kind":         "shell",
			"status":       "done",
			"arguments":    map[string]any{"command": "git status"},
		},
	}, "")
	if display == nil || display.ID != "tool-2" || display.Title != "git status" || display.Command != "git status" || display.Status != "completed" {
		t.Fatalf("fallback display = %#v", display)
	}
	if got := ToolDisplayCommand(map[string]any{"rawInput": map[string]any{"command": "pwd"}}); got != "pwd" {
		t.Fatalf("display command = %q", got)
	}
	if got := ToolDisplayTarget(nil); got != "" {
		t.Fatalf("nil target = %q", got)
	}
	if got := ToolCallIDFromData(nil); got != "" {
		t.Fatalf("nil tool call ID = %q", got)
	}
	if got := PermissionToolDisplay("", nil, "bogus"); got.Title != "permission requested" || got.Status != "" {
		t.Fatalf("permission display = %#v", got)
	}
	for input, want := range map[string]string{"default": "agent", "ask": "ask", "debug": "debug", "custom": "custom"} {
		if got := humanModeLabel(input); got != want {
			t.Fatalf("mode %q = %q", input, got)
		}
	}
}
