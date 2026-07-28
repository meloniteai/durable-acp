package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Spec struct {
	Command         string
	Args            []string
	Dir             string
	Env             []string
	Stderr          io.Writer
	OnNotify        func(Message)
	OnServerRequest func(Message) (any, error)
}

type Process struct {
	cmd             *exec.Cmd
	stdin           io.WriteCloser
	pending         map[string]chan processResponse
	mu              sync.Mutex
	writeMu         sync.Mutex
	nextID          atomic.Uint64
	done            chan struct{}
	doneOnce        sync.Once
	serverRequestMu sync.RWMutex
	serverRequest   func(Message) (any, error)
	exitCode        atomic.Int64
	stderrBuf       boundedBuffer
	stderrDone      chan struct{}
	intentional     atomic.Bool
}

type Message struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type boundedBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.max <= 0 {
		b.max = 8 * 1024
	}
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		b.buf = append(b.buf[:0:0], b.buf[len(b.buf)-b.max:]...)
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

type processResponse struct {
	result json.RawMessage
	err    error
}

func Start(ctx context.Context, spec Spec) (*Process, error) {
	stderr := spec.Stderr
	if stderr == nil {
		stderr = io.Discard
	}
	//nolint:gosec // Executing the caller-supplied process is the transport's purpose.
	cmd := exec.CommandContext(ctx, spec.Command, spec.Args...)
	if strings.TrimSpace(spec.Dir) != "" {
		cmd.Dir = spec.Dir
	}
	if len(spec.Env) > 0 {
		cmd.Env = spec.Env
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	p := &Process{
		cmd:           cmd,
		stdin:         stdin,
		pending:       map[string]chan processResponse{},
		done:          make(chan struct{}),
		serverRequest: spec.OnServerRequest,
		stderrDone:    make(chan struct{}),
	}
	p.exitCode.Store(-1)
	p.stderrBuf.max = 8 * 1024
	cmd.Stderr = io.MultiWriter(stderr, &p.stderrBuf)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go p.readLoop(stdout, spec.OnNotify)
	go func() {
		err := cmd.Wait()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			p.exitCode.Store(int64(exitErr.ExitCode()))
		} else if err == nil {
			p.exitCode.Store(0)
		}
		close(p.stderrDone)
		p.closePending(err)
	}()
	return p, nil
}

func (p *Process) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := strconv.FormatUint(p.nextID.Add(1), 10)
	rawParams, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	resp := make(chan processResponse, 1)
	p.mu.Lock()
	p.pending[id] = resp
	p.mu.Unlock()
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  json.RawMessage(rawParams),
	}
	raw, err := json.Marshal(req)
	if err != nil {
		p.removePending(id)
		return nil, err
	}
	select {
	case <-p.done:
		p.removePending(id)
		return nil, errors.New("agent process exited")
	default:
	}
	p.writeMu.Lock()
	_, err = p.stdin.Write(append(raw, '\n'))
	p.writeMu.Unlock()
	if err != nil {
		p.removePending(id)
		select {
		case <-p.done:
			return nil, errors.New("agent process exited")
		default:
		}
		return nil, err
	}
	select {
	case <-ctx.Done():
		p.removePending(id)
		return nil, ctx.Err()
	case <-p.done:
		return nil, errors.New("agent process exited")
	case got := <-resp:
		return got.result, got.err
	}
}

func (p *Process) Notify(method string, params any) error {
	rawParams, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  json.RawMessage(rawParams),
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	p.writeMu.Lock()
	_, err = p.stdin.Write(append(raw, '\n'))
	p.writeMu.Unlock()
	return err
}

func (p *Process) Close() error {
	p.intentional.Store(true)
	p.doneOnce.Do(func() {
		_ = p.stdin.Close()
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		close(p.done)
	})
	return nil
}

func (p *Process) ExitCode() int {
	return int(p.exitCode.Load())
}

func (p *Process) StderrTail(timeout time.Duration) string {
	select {
	case <-p.stderrDone:
	case <-time.After(timeout):
	}
	return p.stderrBuf.String()
}

func IDString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return strings.TrimSpace(asString)
	}
	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return asNumber.String()
	}
	return strings.TrimSpace(string(raw))
}

func (p *Process) Done() <-chan struct{} {
	return p.done
}

func (p *Process) Intentional() bool {
	return p.intentional.Load()
}

func (p *Process) IsDone() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *Process) SetOnServerRequest(handler func(Message) (any, error)) {
	p.serverRequestMu.Lock()
	p.serverRequest = handler
	p.serverRequestMu.Unlock()
}

func (p *Process) writeResponse(id json.RawMessage, result any, responseErr error) error {
	var idValue any
	if len(id) > 0 {
		if err := json.Unmarshal(id, &idValue); err != nil {
			return err
		}
	}
	resp := map[string]any{"jsonrpc": "2.0", "id": idValue}
	if responseErr != nil {
		resp["error"] = map[string]any{"code": -32000, "message": responseErr.Error()}
	} else {
		resp["result"] = result
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_, err = p.stdin.Write(append(raw, '\n'))
	return err
}

func (p *Process) readLoop(stdout io.Reader, notify func(Message)) {
	reader := bufio.NewReaderSize(stdout, 64*1024)
	var readErr error
	for {
		line, err := reader.ReadBytes('\n')
		payload := bytes.TrimSpace(line)
		if len(payload) > 0 {
			var msg Message
			if unmarshalErr := json.Unmarshal(payload, &msg); unmarshalErr == nil {
				switch {
				case msg.Method != "" && len(msg.ID) > 0:
					var result any
					reqErr := fmt.Errorf("unsupported server request %q", msg.Method)
					p.serverRequestMu.RLock()
					handler := p.serverRequest
					p.serverRequestMu.RUnlock()
					if handler != nil {
						result, reqErr = handler(msg)
					}
					_ = p.writeResponse(msg.ID, result, reqErr)
				case msg.Method != "" && notify != nil:
					notify(msg)
				case len(msg.ID) > 0:
					id := strings.Trim(string(msg.ID), `"`)
					ch := p.removePending(id)
					if ch != nil {
						if msg.Error != nil {
							ch <- processResponse{err: errors.New(msg.Error.Message)}
						} else {
							ch <- processResponse{result: msg.Result}
						}
					}
				case notify != nil:
					notify(msg)
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	p.closePending(readErr)
}

func (p *Process) removePending(id string) chan processResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := p.pending[id]
	delete(p.pending, id)
	return ch
}

func (p *Process) closePending(err error) {
	p.doneOnce.Do(func() {
		close(p.done)
	})
	if err == nil {
		err = errors.New("agent process exited")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, ch := range p.pending {
		ch <- processResponse{err: err}
		delete(p.pending, id)
	}
}
