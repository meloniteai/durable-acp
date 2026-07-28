package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCall(t *testing.T) {
	proc := startHelper(t, nil, nil)
	defer func() { _ = proc.Close() }()

	got, err := proc.Call(context.Background(), "echo", map[string]any{"value": "hello"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	var result map[string]string
	if err := json.Unmarshal(got, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["value"] != "hello" {
		t.Fatalf("result value = %q, want hello", result["value"])
	}
}

func TestNotify(t *testing.T) {
	notified := make(chan Message, 1)
	proc := startHelper(t, func(msg Message) { notified <- msg }, nil)
	defer func() { _ = proc.Close() }()

	if err := proc.Notify("ping", map[string]any{"value": "hello"}); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	select {
	case msg := <-notified:
		if msg.Method != "observed/ping" || !strings.Contains(string(msg.Params), "hello") {
			t.Fatalf("notification = %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for notification")
	}
}

func TestServerRequest(t *testing.T) {
	proc := startHelper(t, nil, nil)
	proc.SetOnServerRequest(func(msg Message) (any, error) {
		if msg.Method != "client/answer" || IDString(msg.ID) != "42" {
			t.Fatalf("server request = %+v", msg)
		}
		return map[string]any{"answer": "yes"}, nil
	})
	defer func() { _ = proc.Close() }()

	got, err := proc.Call(context.Background(), "request", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(got) != `{"answer":"yes"}` {
		t.Fatalf("result = %s", got)
	}
}

func TestClose(t *testing.T) {
	proc := startHelper(t, nil, nil)
	if err := proc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-proc.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("process did not close")
	}
	if !proc.Intentional() {
		t.Fatal("Intentional = false after Close")
	}
	if _, err := proc.Call(context.Background(), "echo", nil); err == nil {
		t.Fatal("Call after Close error = nil")
	}
}

func TestChildExit(t *testing.T) {
	proc := startHelper(t, nil, nil)
	_, err := proc.Call(context.Background(), "exit", nil)
	if err == nil {
		t.Fatal("Call error = nil")
	}
	select {
	case <-proc.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("process did not exit")
	}
	deadline := time.Now().Add(2 * time.Second)
	for proc.ExitCode() == -1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if proc.ExitCode() != 7 {
		t.Fatalf("ExitCode = %d, want 7", proc.ExitCode())
	}
	if got := proc.StderrTail(time.Second); got != "helper failed\n" {
		t.Fatalf("StderrTail = %q", got)
	}
}

func TestIDString(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{raw: `42`, want: "42"},
		{raw: `"perm-1"`, want: "perm-1"},
		{raw: ` " spaced " `, want: "spaced"},
		{raw: `true`, want: "true"},
		{raw: ``, want: ""},
	}
	for _, tc := range tests {
		if got := IDString(json.RawMessage(tc.raw)); got != tc.want {
			t.Errorf("IDString(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("DURABLE_ACP_TRANSPORT_HELPER") != "1" {
		return
	}
	reader := bufio.NewReader(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			os.Exit(0)
		}
		var msg Message
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}
		switch msg.Method {
		case "echo":
			writeHelperResponse(encoder, msg.ID, msg.Params)
		case "ping":
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "observed/ping", "params": msg.Params})
		case "request":
			_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": 42, "method": "client/answer", "params": map[string]any{"question": "continue?"}})
			responseLine, readErr := reader.ReadBytes('\n')
			if readErr != nil {
				os.Exit(2)
			}
			var response Message
			if err := json.Unmarshal(responseLine, &response); err != nil {
				os.Exit(3)
			}
			if IDString(response.ID) != "42" {
				os.Exit(6)
			}
			writeHelperResponse(encoder, msg.ID, response.Result)
		case "exit":
			_, _ = fmt.Fprintln(os.Stderr, "helper failed")
			os.Exit(7)
		}
	}
}

func startHelper(t *testing.T, notify func(Message), serverRequest func(Message) (any, error)) *Process {
	t.Helper()
	proc, err := Start(context.Background(), Spec{
		Command:         os.Args[0],
		Args:            []string{"-test.run=^TestHelperProcess$"},
		Env:             append(os.Environ(), "DURABLE_ACP_TRANSPORT_HELPER=1"),
		OnNotify:        notify,
		OnServerRequest: serverRequest,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return proc
}

func writeHelperResponse(encoder *json.Encoder, id json.RawMessage, result json.RawMessage) {
	var idValue any
	if err := json.Unmarshal(id, &idValue); err != nil {
		os.Exit(4)
	}
	var resultValue any
	if err := json.Unmarshal(result, &resultValue); err != nil {
		os.Exit(5)
	}
	_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": idValue, "result": resultValue})
}
