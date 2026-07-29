package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fixtureBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "durable-acp-transport-")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fixtureBinary = filepath.Join(dir, "rpcserver")
	if runtime.GOOS == "windows" {
		fixtureBinary += ".exe"
	}
	//nolint:gosec // TestMain compiles the repository-owned fixture.
	cmd := exec.CommandContext(context.Background(), "go", "build", "-o", fixtureBinary, "./testdata/rpcserver")
	if output, buildErr := cmd.CombinedOutput(); buildErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "build transport fixture: %v\n%s", buildErr, output)
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

func TestCall(t *testing.T) {
	proc := startFixture(t, Spec{})

	result, err := proc.Call(context.Background(), "echo", map[string]any{"value": "hello"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"value":"hello"}`, string(result))

	_, err = proc.Call(context.Background(), "fail", nil)
	require.EqualError(t, err, "fixture failure")
	var rpcErr *RPCError
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, -32000, rpcErr.Code)
	assert.JSONEq(t, `{"retryable":false}`, string(rpcErr.Data))
}

func TestNotify(t *testing.T) {
	notified := make(chan Message, 1)
	proc := startFixture(t, Spec{OnNotify: func(msg Message) { notified <- msg }})

	require.NoError(t, proc.Notify("ping", map[string]any{"value": "hello"}))
	select {
	case msg := <-notified:
		assert.Equal(t, "observed/ping", msg.Method)
		assert.JSONEq(t, `{"value":"hello"}`, string(msg.Params))
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for notification")
	}
}

func TestObserverSeesWireOrder(t *testing.T) {
	var observed []string
	proc := startFixture(t, Spec{
		Observe: func(_ context.Context, direction Direction, raw json.RawMessage) error {
			observed = append(observed, string(direction)+":"+string(raw))
			return nil
		},
	})

	_, err := proc.Call(context.Background(), "echo", map[string]bool{"ok": true})
	require.NoError(t, err)
	require.Len(t, observed, 2)
	assert.Contains(t, observed[0], "outbound:")
	assert.Contains(t, observed[0], `"method":"echo"`)
	assert.Contains(t, observed[1], "inbound:")
	assert.Contains(t, observed[1], `"result":{"ok":true}`)
}

func TestObserverErrorFailsCall(t *testing.T) {
	want := errors.New("observer failed")
	proc := startFixture(t, Spec{
		Observe: func(_ context.Context, direction Direction, _ json.RawMessage) error {
			if direction == DirectionOutbound {
				return want
			}
			return nil
		},
	})

	_, err := proc.Call(context.Background(), "echo", nil)
	require.ErrorIs(t, err, want)
}

func TestServerRequest(t *testing.T) {
	requests := make(chan Message, 1)
	proc := startFixture(t, Spec{})
	proc.SetOnServerRequest(func(msg Message) (any, error) {
		requests <- msg
		return map[string]any{"answer": "yes"}, nil
	})

	result, err := proc.Call(context.Background(), "request", nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"answer":"yes"}`, string(result))
	msg := <-requests
	assert.Equal(t, "client/answer", msg.Method)
	assert.Equal(t, "42", IDString(msg.ID))
}

func TestCallCancellationNotifiesPeer(t *testing.T) {
	proc := startFixture(t, Spec{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := proc.Call(ctx, "wait-cancel", nil)
	require.ErrorIs(t, err, context.Canceled)

	result, err := proc.Call(context.Background(), "echo", map[string]bool{"usable": true})
	require.NoError(t, err)
	assert.JSONEq(t, `{"usable":true}`, string(result))
}

func TestServerRequestCancellation(t *testing.T) {
	proc := startFixture(t, Spec{
		OnRequest: func(ctx context.Context, msg Message) (any, error) {
			assert.Equal(t, "client/wait", msg.Method)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	result, err := proc.Call(context.Background(), "request-cancel", nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"cancelled":true}`, string(result))
}

func TestClose(t *testing.T) {
	proc := startFixture(t, Spec{})
	assert.False(t, proc.IsDone())

	require.NoError(t, proc.Close())
	assert.True(t, proc.IsDone())
	assert.True(t, proc.Intentional())
	select {
	case <-proc.Done():
	default:
		require.FailNow(t, "Done channel remains open after Close")
	}
	_, err := proc.Call(context.Background(), "echo", nil)
	require.Error(t, err)
}

func TestChildExit(t *testing.T) {
	proc := startFixture(t, Spec{})
	_, err := proc.Call(context.Background(), "exit", nil)
	require.Error(t, err)
	require.Eventually(t, func() bool { return proc.ExitCode() == 7 }, 2*time.Second, time.Millisecond)
	assert.Equal(t, "fixture failed\n", proc.StderrTail(time.Second))
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
	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			assert.Equal(t, test.want, IDString(json.RawMessage(test.raw)))
		})
	}
}

func TestRPCError(t *testing.T) {
	err := error(&RPCError{Code: CodeInvalidParams, Message: "bad input"})
	var rpcErr *RPCError
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, "bad input", rpcErr.Error())
	assert.Empty(t, (*RPCError)(nil).Error())
}

func startFixture(t *testing.T, spec Spec) *Process {
	t.Helper()
	spec.Command = fixtureBinary
	proc, err := Start(context.Background(), spec)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proc.Close()) })
	return proc
}

func TestStderrTailIsBounded(t *testing.T) {
	proc := startFixture(t, Spec{})
	_, err := proc.Call(context.Background(), "large-stderr", nil)
	require.Error(t, err)
	tail := proc.StderrTail(time.Second)
	assert.Len(t, tail, 8*1024)
	assert.True(t, strings.HasSuffix(tail, "tail\n"))
}
