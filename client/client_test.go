package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/transport"
)

var fixtureBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "durable-acp-client-")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fixtureBinary = filepath.Join(dir, "rpcserver")
	if runtime.GOOS == "windows" {
		fixtureBinary += ".exe"
	}
	//nolint:gosec // TestMain compiles the repository-owned fixture.
	cmd := exec.CommandContext(
		context.Background(),
		"go",
		"build",
		"-o",
		fixtureBinary,
		"../transport/testdata/rpcserver",
	)
	if output, buildErr := cmd.CombinedOutput(); buildErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "build ACP fixture: %v\n%s", buildErr, output)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	if err := os.RemoveAll(dir); err != nil && code == 0 {
		_, _ = fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func TestClientCoversStableACPMethods(t *testing.T) {
	handler := &completeHandler{calls: map[string]int{}}
	handlerErrors := make(chan error, 1)
	client, err := Start(context.Background(), Spec{
		Command:        fixtureBinary,
		Handler:        handler,
		OnHandlerError: func(err error) { handlerErrors <- err },
		Initialize: acp.InitializeRequest{
			ClientInfo: &acp.Implementation{Name: "durable-acp-test", Version: "dev"},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	initialized := client.InitializeResponse()
	require.NotNil(t, initialized)
	assert.Equal(t, acp.ProtocolVersion(acp.ProtocolVersionNumber), initialized.ProtocolVersion)
	assert.Equal(t, "fixture", initialized.AgentInfo.Name)

	_, err = client.Authenticate(context.Background(), &acp.AuthenticateRequest{MethodId: "test"})
	require.NoError(t, err)
	session, err := client.NewSession(context.Background(), &acp.NewSessionRequest{
		Cwd:        "/tmp",
		McpServers: []acp.McpServer{},
	})
	require.NoError(t, err)
	assert.Equal(t, acp.SessionId("fixture-session"), session.SessionId)
	_, err = client.LoadSession(context.Background(), &acp.LoadSessionRequest{})
	require.NoError(t, err)
	_, err = client.ResumeSession(context.Background(), &acp.ResumeSessionRequest{})
	require.NoError(t, err)
	list, err := client.ListSessions(context.Background(), &acp.ListSessionsRequest{})
	require.NoError(t, err)
	assert.Empty(t, list.Sessions)
	_, err = client.SetSessionMode(context.Background(), &acp.SetSessionModeRequest{})
	require.NoError(t, err)
	configRequest := acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{
			SessionId: acp.SessionId("fixture-session"),
			ConfigId:  acp.SessionConfigId("model"),
			Value:     acp.SessionConfigValueId("test"),
		},
	}
	_, err = client.SetSessionConfigOption(context.Background(), &configRequest)
	require.NoError(t, err)

	prompt, err := client.Prompt(context.Background(), &acp.PromptRequest{
		SessionId: acp.SessionId("fixture-session"),
		Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
	})
	require.NoError(t, err)
	assert.Equal(t, acp.StopReasonEndTurn, prompt.StopReason)
	require.NoError(t, client.Cancel(
		context.Background(),
		&acp.CancelNotification{SessionId: acp.SessionId("fixture-session")},
	))

	extension, err := client.CallExtension(
		context.Background(),
		"_fixture/echo",
		map[string]bool{"ok": true},
	)
	require.NoError(t, err)
	assert.JSONEq(t, `{"ok":true}`, string(extension))
	require.NoError(t, client.NotifyExtension(
		context.Background(),
		"_fixture/notify",
		map[string]bool{"ok": true},
	))
	_, err = client.CallExtension(context.Background(), "fixture/invalid", nil)
	require.Error(t, err)

	_, err = client.CloseSession(context.Background(), &acp.CloseSessionRequest{})
	require.NoError(t, err)
	_, err = client.DeleteSession(context.Background(), &acp.DeleteSessionRequest{})
	require.NoError(t, err)
	_, err = client.Logout(context.Background(), &acp.LogoutRequest{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return handler.callCount("extension_notification") == 1
	}, time.Second, time.Millisecond)
	assert.Equal(t, 1, handler.callCount("session_update"))
	assert.Equal(t, 1, handler.callCount("permission"))
	assert.Equal(t, 1, handler.callCount("read_file"))
	assert.Equal(t, 1, handler.callCount("write_file"))
	assert.Equal(t, 1, handler.callCount("terminal_create"))
	assert.Equal(t, 1, handler.callCount("terminal_output"))
	assert.Equal(t, 1, handler.callCount("terminal_wait"))
	assert.Equal(t, 1, handler.callCount("terminal_kill"))
	assert.Equal(t, 1, handler.callCount("terminal_release"))
	assert.Equal(t, 1, handler.callCount("elicitation_create"))
	assert.Equal(t, 1, handler.callCount("elicitation_complete"))
	assert.Equal(t, 1, handler.callCount("extension_request"))
	select {
	case handlerErr := <-handlerErrors:
		t.Fatalf("unexpected handler error: %v", handlerErr)
	default:
	}
}

func TestClientRejectsUnsupportedRequestedProtocolVersion(t *testing.T) {
	request := acp.InitializeRequest{ProtocolVersion: 2}
	client, err := Start(context.Background(), Spec{
		Command:    fixtureBinary,
		Initialize: request,
	})
	require.Nil(t, client)
	require.ErrorIs(t, err, ErrUnsupportedProtocolVersion)
	assert.Equal(t, acp.ProtocolVersion(2), request.ProtocolVersion)
	require.Error(t, validateExtensionMethod("not-an-extension"))
	assert.NoError(t, validateExtensionMethod("_example/test"))
}

func TestInitializeParamsMergesExtensionFields(t *testing.T) {
	params, err := initializeParams(acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{
			Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}},
		},
	}, map[string]any{
		"capabilities": map[string]any{},
	}, map[string]any{
		"plan": map[string]any{},
	})
	require.NoError(t, err)
	assert.InDelta(t, acp.ProtocolVersionNumber, params["protocolVersion"], 0)
	assert.IsType(t, map[string]any{}, params["capabilities"])
	capabilities, ok := params["clientCapabilities"].(map[string]any)
	require.True(t, ok)
	assert.IsType(t, map[string]any{}, capabilities["plan"])
	assert.IsType(t, map[string]any{}, capabilities["elicitation"])
}

func TestNormalizeInitializeResponseAcceptsBooleanSessionCapabilities(t *testing.T) {
	raw, err := normalizeInitializeResponse(json.RawMessage(`{"agentCapabilities":{"sessionCapabilities":{"resume":true,"close":false}}}`))
	require.NoError(t, err)
	var response acp.InitializeResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	require.NotNil(t, response.AgentCapabilities.SessionCapabilities.Resume)
	assert.Nil(t, response.AgentCapabilities.SessionCapabilities.Close)
}

func TestClientRejectsUnsupportedAgentProtocolVersion(t *testing.T) {
	client, err := Start(context.Background(), Spec{
		Command: fixtureBinary,
		Args:    []string{"unsupported-protocol"},
	})
	require.Nil(t, client)
	require.ErrorIs(t, err, ErrUnsupportedProtocolVersion)
}

func TestClientReturnsInitializationFailure(t *testing.T) {
	client, err := Start(context.Background(), Spec{
		Command: fixtureBinary,
		Args:    []string{"initialize-error"},
	})
	require.Nil(t, client)
	require.ErrorContains(t, err, acp.MethodInitialize)
	require.ErrorContains(t, err, "fixture initialization failure")
}

func TestUnsupportedClientRequest(t *testing.T) {
	client := &Connection{}
	_, err := client.handleRequest(context.Background(), transport.Message{
		Method: "unknown",
		Params: json.RawMessage(`{}`),
	})
	var rpcErr *transport.RPCError
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, transport.CodeMethodNotFound, rpcErr.Code)
	assert.Contains(t, rpcErr.Message, ErrUnsupportedMethod.Error())
}

func TestClientCanForwardLegacyProviderExtensions(t *testing.T) {
	handler := &completeHandler{calls: map[string]int{}}
	client := &Connection{handler: handler, legacyExtensions: true}
	result, err := client.handleRequest(context.Background(), transport.Message{
		Method: "provider/request",
		Params: json.RawMessage(`{"value":true}`),
	})
	require.NoError(t, err)
	raw, ok := result.(json.RawMessage)
	require.True(t, ok)
	assert.JSONEq(t, `{"value":true}`, string(raw))
	client.handleNotification(transport.Message{Method: "provider/update", Params: json.RawMessage(`{"value":true}`)})
	assert.Equal(t, 1, handler.callCount("extension_request"))
	assert.Equal(t, 1, handler.callCount("extension_notification"))
}

func TestClientRejectsInvalidCallbackParameters(t *testing.T) {
	client := &Connection{handler: &completeHandler{calls: map[string]int{}}}
	_, err := client.handleRequest(context.Background(), transport.Message{
		Method: acp.MethodElicitationCreate,
		Params: json.RawMessage(`{}`),
	})
	var rpcErr *transport.RPCError
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, transport.CodeInvalidParams, rpcErr.Code)
}

func TestClientRejectsUnsupportedStandardCallbacks(t *testing.T) {
	t.Parallel()

	client := &Connection{}
	for _, method := range []string{
		acp.MethodSessionRequestPermission,
		acp.MethodFsReadTextFile,
		acp.MethodFsWriteTextFile,
		acp.MethodTerminalCreate,
		acp.MethodTerminalOutput,
		acp.MethodTerminalWaitForExit,
		acp.MethodTerminalKill,
		acp.MethodTerminalRelease,
		acp.MethodElicitationCreate,
		"_fixture/request",
	} {
		_, err := client.handleRequest(context.Background(), transport.Message{
			Method: method,
			Params: json.RawMessage(`{}`),
		})
		var rpcErr *transport.RPCError
		require.ErrorAs(t, err, &rpcErr, method)
		assert.Contains(t, rpcErr.Message, ErrUnsupportedMethod.Error(), method)
	}
}

func TestClientReportsNotificationErrors(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("handler failed")
	var reported []error
	client := &Connection{
		handler: failingNotificationHandler{err: handlerErr},
		onHandlerError: func(err error) {
			reported = append(reported, err)
		},
	}
	client.handleNotification(transport.Message{
		Method: acp.MethodSessionUpdate,
		Params: json.RawMessage(
			`{"sessionId":"fixture-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}}`,
		),
	})
	client.handleNotification(transport.Message{
		Method: acp.MethodElicitationComplete,
		Params: json.RawMessage(`{"elicitationId":"elicit-1"}`),
	})
	client.handleNotification(transport.Message{
		Method: "_fixture/notification",
		Params: json.RawMessage(`{"ok":true}`),
	})
	client.handleNotification(transport.Message{
		Method: acp.MethodSessionUpdate,
		Params: json.RawMessage(`{`),
	})
	client.handleNotification(transport.Message{Method: "unknown"})

	require.Len(t, reported, 5)
	require.ErrorIs(t, reported[0], handlerErr)
	require.ErrorIs(t, reported[1], handlerErr)
	require.ErrorIs(t, reported[2], handlerErr)
	var rpcErr *transport.RPCError
	require.ErrorAs(t, reported[3], &rpcErr)
	assert.Equal(t, transport.CodeInvalidParams, rpcErr.Code)
	var unsupportedErr *transport.RPCError
	require.ErrorAs(t, reported[4], &unsupportedErr)
	assert.Contains(t, unsupportedErr.Message, ErrUnsupportedMethod.Error())

	ignored := &Connection{}
	ignored.handleNotification(transport.Message{Method: "_fixture/ignored"})
	ignored.handleNotification(transport.Message{Method: acp.MethodSessionUpdate})
}

func TestConnectionProcessAccessors(t *testing.T) {
	t.Parallel()

	connection, err := Start(context.Background(), Spec{Command: fixtureBinary})
	require.NoError(t, err)
	assert.NotNil(t, connection.Done())
	assert.Equal(t, -1, connection.ExitCode())
	assert.Empty(t, connection.StderrTail(0))
	require.Error(t, connection.NotifyExtension(context.Background(), "not-an-extension", nil))
	require.NoError(t, connection.Close())
}

func TestStartReturnsProcessFailure(t *testing.T) {
	t.Parallel()

	connection, err := Start(context.Background(), Spec{
		Command: filepath.Join(t.TempDir(), "missing-agent"),
	})
	require.Nil(t, connection)
	require.Error(t, err)
}

func TestClientObservesTheWireConversation(t *testing.T) {
	type observedMessage struct {
		direction transport.Direction
		message   json.RawMessage
	}
	var observed []observedMessage
	client, err := Start(context.Background(), Spec{
		Command: fixtureBinary,
		Observe: func(
			_ context.Context,
			direction transport.Direction,
			message json.RawMessage,
		) error {
			observed = append(observed, observedMessage{
				direction: direction,
				message:   append(json.RawMessage(nil), message...),
			})
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	response := client.InitializeResponse()
	require.NotNil(t, response)
	assert.Equal(t, acp.ProtocolVersion(acp.ProtocolVersionNumber), response.ProtocolVersion)

	require.Len(t, observed, 2)
	assert.Equal(t, transport.DirectionOutbound, observed[0].direction)
	assert.JSONEq(
		t,
		`{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientCapabilities":{"fs":{}},"protocolVersion":1}}`,
		string(observed[0].message),
	)
	assert.Equal(t, transport.DirectionInbound, observed[1].direction)
	assert.Contains(t, string(observed[1].message), `"protocolVersion":1`)
}

type failingNotificationHandler struct {
	err error
}

func (h failingNotificationHandler) SessionUpdate(context.Context, *acp.SessionNotification) error {
	return h.err
}

func (h failingNotificationHandler) CreateElicitation(
	context.Context,
	*acp.CreateElicitationRequest,
) (*acp.CreateElicitationResponse, error) {
	return nil, h.err
}

func (h failingNotificationHandler) ElicitationComplete(
	context.Context,
	*acp.CompleteElicitationNotification,
) error {
	return h.err
}

func (h failingNotificationHandler) ExtensionRequest(
	context.Context,
	string,
	json.RawMessage,
) (any, error) {
	return nil, h.err
}

func (h failingNotificationHandler) ExtensionNotification(
	context.Context,
	string,
	json.RawMessage,
) error {
	return h.err
}

type completeHandler struct {
	mu    sync.Mutex
	calls map[string]int
}

func (h *completeHandler) record(name string) {
	h.mu.Lock()
	h.calls[name]++
	h.mu.Unlock()
}

func (h *completeHandler) callCount(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[name]
}

func (h *completeHandler) SessionUpdate(
	_ context.Context,
	notification *acp.SessionNotification,
) error {
	h.record("session_update")
	if notification.SessionId == "" {
		return errors.New("missing session ID")
	}
	return nil
}

func (h *completeHandler) RequestPermission(
	context.Context,
	*acp.RequestPermissionRequest,
) (*acp.RequestPermissionResponse, error) {
	h.record("permission")
	return &acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (h *completeHandler) ReadTextFile(
	context.Context,
	*acp.ReadTextFileRequest,
) (*acp.ReadTextFileResponse, error) {
	h.record("read_file")
	return &acp.ReadTextFileResponse{Content: "input"}, nil
}

func (h *completeHandler) WriteTextFile(
	context.Context,
	*acp.WriteTextFileRequest,
) (*acp.WriteTextFileResponse, error) {
	h.record("write_file")
	return &acp.WriteTextFileResponse{}, nil
}

func (h *completeHandler) CreateTerminal(
	context.Context,
	*acp.CreateTerminalRequest,
) (*acp.CreateTerminalResponse, error) {
	h.record("terminal_create")
	return &acp.CreateTerminalResponse{TerminalId: acp.TerminalId("terminal-1")}, nil
}

func (h *completeHandler) TerminalOutput(
	context.Context,
	*acp.TerminalOutputRequest,
) (*acp.TerminalOutputResponse, error) {
	h.record("terminal_output")
	return &acp.TerminalOutputResponse{Output: "hello"}, nil
}

func (h *completeHandler) WaitForTerminalExit(
	context.Context,
	*acp.WaitForTerminalExitRequest,
) (*acp.WaitForTerminalExitResponse, error) {
	h.record("terminal_wait")
	exitCode := 0
	return &acp.WaitForTerminalExitResponse{ExitCode: &exitCode}, nil
}

func (h *completeHandler) KillTerminal(
	context.Context,
	*acp.KillTerminalRequest,
) (*acp.KillTerminalResponse, error) {
	h.record("terminal_kill")
	return &acp.KillTerminalResponse{}, nil
}

func (h *completeHandler) ReleaseTerminal(
	context.Context,
	*acp.ReleaseTerminalRequest,
) (*acp.ReleaseTerminalResponse, error) {
	h.record("terminal_release")
	return &acp.ReleaseTerminalResponse{}, nil
}

func (h *completeHandler) CreateElicitation(
	context.Context,
	*acp.CreateElicitationRequest,
) (*acp.CreateElicitationResponse, error) {
	h.record("elicitation_create")
	return &acp.CreateElicitationResponse{
		Decline: &acp.CreateElicitationDecline{Action: "decline"},
	}, nil
}

func (h *completeHandler) ElicitationComplete(
	context.Context,
	*acp.CompleteElicitationNotification,
) error {
	h.record("elicitation_complete")
	return nil
}

func (h *completeHandler) ExtensionRequest(
	_ context.Context,
	_ string,
	params json.RawMessage,
) (any, error) {
	h.record("extension_request")
	return params, nil
}

func (h *completeHandler) ExtensionNotification(
	context.Context,
	string,
	json.RawMessage,
) error {
	h.record("extension_notification")
	return nil
}
