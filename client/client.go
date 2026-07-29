package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/meloniteai/durable-acp/acp"
	"github.com/meloniteai/durable-acp/transport"
)

var (
	ErrUnsupportedProtocolVersion = errors.New("unsupported ACP protocol version")
	ErrUnsupportedMethod          = errors.New("unsupported ACP client method")
)

type Spec struct {
	Command        string
	Args           []string
	Dir            string
	Env            []string
	Stderr         io.Writer
	Observe        transport.Observer
	Handler        any
	OnHandlerError func(error)
	Initialize     acp.InitializeRequest
}

type Connection struct {
	process            *transport.Process
	handler            any
	onHandlerError     func(error)
	initializeResponse *acp.InitializeResponse
}

type SessionUpdateHandler interface {
	SessionUpdate(context.Context, *acp.SessionNotification) error
}

type PermissionHandler interface {
	RequestPermission(context.Context, *acp.RequestPermissionRequest) (*acp.RequestPermissionResponse, error)
}

type FileSystemHandler interface {
	ReadTextFile(context.Context, *acp.ReadTextFileRequest) (*acp.ReadTextFileResponse, error)
	WriteTextFile(context.Context, *acp.WriteTextFileRequest) (*acp.WriteTextFileResponse, error)
}

type TerminalHandler interface {
	CreateTerminal(context.Context, *acp.CreateTerminalRequest) (*acp.CreateTerminalResponse, error)
	TerminalOutput(context.Context, *acp.TerminalOutputRequest) (*acp.TerminalOutputResponse, error)
	WaitForTerminalExit(context.Context, *acp.WaitForTerminalExitRequest) (*acp.WaitForTerminalExitResponse, error)
	KillTerminal(context.Context, *acp.KillTerminalRequest) (*acp.KillTerminalResponse, error)
	ReleaseTerminal(context.Context, *acp.ReleaseTerminalRequest) (*acp.ReleaseTerminalResponse, error)
}

type ElicitationHandler interface {
	CreateElicitation(context.Context, *acp.CreateElicitationRequest) (*acp.CreateElicitationResponse, error)
	ElicitationComplete(context.Context, *acp.CompleteElicitationNotification) error
}

type ExtensionHandler interface {
	ExtensionRequest(context.Context, string, json.RawMessage) (any, error)
	ExtensionNotification(context.Context, string, json.RawMessage) error
}

func Start(ctx context.Context, spec Spec) (*Connection, error) {
	request := spec.Initialize
	if request.ProtocolVersion == 0 {
		request.ProtocolVersion = acp.ProtocolVersionNumber
	}
	if request.ProtocolVersion != acp.ProtocolVersionNumber {
		return nil, fmt.Errorf(
			"%w: requested %d, client supports %d",
			ErrUnsupportedProtocolVersion,
			request.ProtocolVersion,
			acp.ProtocolVersionNumber,
		)
	}
	client := &Connection{
		handler:        spec.Handler,
		onHandlerError: spec.OnHandlerError,
	}
	process, err := transport.Start(ctx, transport.Spec{
		Command:   spec.Command,
		Args:      spec.Args,
		Dir:       spec.Dir,
		Env:       spec.Env,
		Stderr:    spec.Stderr,
		Observe:   spec.Observe,
		OnNotify:  client.handleNotification,
		OnRequest: client.handleRequest,
	})
	if err != nil {
		return nil, err
	}
	client.process = process
	response, err := call[acp.InitializeResponse](ctx, process, acp.MethodInitialize, &request)
	if err != nil {
		return nil, errors.Join(err, process.Close())
	}
	if response.ProtocolVersion != acp.ProtocolVersionNumber {
		err = fmt.Errorf(
			"%w: agent selected %d, client supports %d",
			ErrUnsupportedProtocolVersion,
			response.ProtocolVersion,
			acp.ProtocolVersionNumber,
		)
		return nil, errors.Join(err, process.Close())
	}
	client.initializeResponse = response
	return client, nil
}

func (c *Connection) InitializeResponse() *acp.InitializeResponse {
	return c.initializeResponse
}

func (c *Connection) Authenticate(
	ctx context.Context,
	request *acp.AuthenticateRequest,
) (*acp.AuthenticateResponse, error) {
	return call[acp.AuthenticateResponse](ctx, c.process, acp.MethodAuthenticate, request)
}

func (c *Connection) Logout(
	ctx context.Context,
	request *acp.LogoutRequest,
) (*acp.LogoutResponse, error) {
	return call[acp.LogoutResponse](ctx, c.process, acp.MethodLogout, request)
}

func (c *Connection) NewSession(
	ctx context.Context,
	request *acp.NewSessionRequest,
) (*acp.NewSessionResponse, error) {
	return call[acp.NewSessionResponse](ctx, c.process, acp.MethodSessionNew, request)
}

func (c *Connection) LoadSession(
	ctx context.Context,
	request *acp.LoadSessionRequest,
) (*acp.LoadSessionResponse, error) {
	return call[acp.LoadSessionResponse](ctx, c.process, acp.MethodSessionLoad, request)
}

func (c *Connection) ResumeSession(
	ctx context.Context,
	request *acp.ResumeSessionRequest,
) (*acp.ResumeSessionResponse, error) {
	return call[acp.ResumeSessionResponse](ctx, c.process, acp.MethodSessionResume, request)
}

func (c *Connection) ListSessions(
	ctx context.Context,
	request *acp.ListSessionsRequest,
) (*acp.ListSessionsResponse, error) {
	return call[acp.ListSessionsResponse](ctx, c.process, acp.MethodSessionList, request)
}

func (c *Connection) DeleteSession(
	ctx context.Context,
	request *acp.DeleteSessionRequest,
) (*acp.DeleteSessionResponse, error) {
	return call[acp.DeleteSessionResponse](ctx, c.process, acp.MethodSessionDelete, request)
}

func (c *Connection) CloseSession(
	ctx context.Context,
	request *acp.CloseSessionRequest,
) (*acp.CloseSessionResponse, error) {
	return call[acp.CloseSessionResponse](ctx, c.process, acp.MethodSessionClose, request)
}

func (c *Connection) SetSessionMode(
	ctx context.Context,
	request *acp.SetSessionModeRequest,
) (*acp.SetSessionModeResponse, error) {
	return call[acp.SetSessionModeResponse](ctx, c.process, acp.MethodSessionSetMode, request)
}

func (c *Connection) SetSessionConfigOption(
	ctx context.Context,
	request *acp.SetSessionConfigOptionRequest,
) (*acp.SetSessionConfigOptionResponse, error) {
	return call[acp.SetSessionConfigOptionResponse](
		ctx,
		c.process,
		acp.MethodSessionSetConfigOption,
		request,
	)
}

func (c *Connection) Prompt(
	ctx context.Context,
	request *acp.PromptRequest,
) (*acp.PromptResponse, error) {
	return call[acp.PromptResponse](ctx, c.process, acp.MethodSessionPrompt, request)
}

func (c *Connection) Cancel(ctx context.Context, request *acp.CancelNotification) error {
	return c.process.NotifyContext(ctx, acp.MethodSessionCancel, request)
}

func (c *Connection) CallExtension(
	ctx context.Context,
	method string,
	params any,
) (json.RawMessage, error) {
	if err := validateExtensionMethod(method); err != nil {
		return nil, err
	}
	return c.process.Call(ctx, method, params)
}

func (c *Connection) NotifyExtension(
	ctx context.Context,
	method string,
	params any,
) error {
	if err := validateExtensionMethod(method); err != nil {
		return err
	}
	return c.process.NotifyContext(ctx, method, params)
}

func (c *Connection) Close() error {
	return c.process.Close()
}

func (c *Connection) Done() <-chan struct{} {
	return c.process.Done()
}

func (c *Connection) ExitCode() int {
	return c.process.ExitCode()
}

func (c *Connection) StderrTail(timeout time.Duration) string {
	return c.process.StderrTail(timeout)
}

func (c *Connection) handleRequest(
	ctx context.Context,
	message transport.Message,
) (any, error) {
	switch message.Method {
	case acp.MethodSessionRequestPermission:
		handler, ok := c.handler.(PermissionHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.RequestPermissionRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.RequestPermission(ctx, request)
	case acp.MethodFsReadTextFile:
		handler, ok := c.handler.(FileSystemHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.ReadTextFileRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.ReadTextFile(ctx, request)
	case acp.MethodFsWriteTextFile:
		handler, ok := c.handler.(FileSystemHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.WriteTextFileRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.WriteTextFile(ctx, request)
	case acp.MethodTerminalCreate:
		handler, ok := c.handler.(TerminalHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.CreateTerminalRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.CreateTerminal(ctx, request)
	case acp.MethodTerminalOutput:
		handler, ok := c.handler.(TerminalHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.TerminalOutputRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.TerminalOutput(ctx, request)
	case acp.MethodTerminalWaitForExit:
		handler, ok := c.handler.(TerminalHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.WaitForTerminalExitRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.WaitForTerminalExit(ctx, request)
	case acp.MethodTerminalKill:
		handler, ok := c.handler.(TerminalHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.KillTerminalRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.KillTerminal(ctx, request)
	case acp.MethodTerminalRelease:
		handler, ok := c.handler.(TerminalHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.ReleaseTerminalRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.ReleaseTerminal(ctx, request)
	case acp.MethodElicitationCreate:
		handler, ok := c.handler.(ElicitationHandler)
		if !ok {
			return nil, unsupported(message.Method)
		}
		request, err := decodeParams[acp.CreateElicitationRequest](message.Params)
		if err != nil {
			return nil, err
		}
		return handler.CreateElicitation(ctx, request)
	default:
		if strings.HasPrefix(message.Method, "_") {
			handler, ok := c.handler.(ExtensionHandler)
			if !ok {
				return nil, unsupported(message.Method)
			}
			return handler.ExtensionRequest(ctx, message.Method, message.Params)
		}
		return nil, unsupported(message.Method)
	}
}

func (c *Connection) handleNotification(message transport.Message) {
	ctx := context.Background()
	var err error
	switch message.Method {
	case acp.MethodSessionUpdate:
		handler, ok := c.handler.(SessionUpdateHandler)
		if !ok {
			err = unsupported(message.Method)
			break
		}
		var request *acp.SessionNotification
		request, err = decodeParams[acp.SessionNotification](message.Params)
		if err == nil {
			err = handler.SessionUpdate(ctx, request)
		}
	case acp.MethodElicitationComplete:
		handler, ok := c.handler.(ElicitationHandler)
		if !ok {
			err = unsupported(message.Method)
			break
		}
		var request *acp.CompleteElicitationNotification
		request, err = decodeParams[acp.CompleteElicitationNotification](message.Params)
		if err == nil {
			err = handler.ElicitationComplete(ctx, request)
		}
	default:
		if !strings.HasPrefix(message.Method, "_") {
			err = unsupported(message.Method)
			break
		}
		handler, ok := c.handler.(ExtensionHandler)
		if ok {
			err = handler.ExtensionNotification(ctx, message.Method, message.Params)
		}
	}
	if err != nil && c.onHandlerError != nil {
		c.onHandlerError(err)
	}
}

func call[T any](
	ctx context.Context,
	process *transport.Process,
	method string,
	params any,
) (*T, error) {
	raw, err := process.Call(ctx, method, params)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	var response T
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("%s response: %w", method, err)
	}
	return &response, nil
}

func decodeParams[T any](raw json.RawMessage) (*T, error) {
	var request T
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, &transport.RPCError{
			Code:    transport.CodeInvalidParams,
			Message: err.Error(),
		}
	}
	if validator, ok := any(&request).(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return nil, &transport.RPCError{
				Code:    transport.CodeInvalidParams,
				Message: err.Error(),
			}
		}
	}
	return &request, nil
}

func unsupported(method string) error {
	return &transport.RPCError{
		Code:    transport.CodeMethodNotFound,
		Message: fmt.Sprintf("%s: %s", ErrUnsupportedMethod, method),
	}
}

func validateExtensionMethod(method string) error {
	if !strings.HasPrefix(method, "_") {
		return fmt.Errorf("ACP extension methods must start with _: %q", method)
	}
	return nil
}
