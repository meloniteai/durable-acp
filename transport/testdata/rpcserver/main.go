package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

type message struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var msg message
		if json.Unmarshal(line, &msg) != nil {
			continue
		}
		switch msg.Method {
		case "initialize":
			if len(os.Args) > 1 && os.Args[1] == "initialize-error" {
				encode(encoder, message{
					JSONRPC: "2.0",
					ID:      msg.ID,
					Error: &rpcError{
						Code:    -32000,
						Message: "fixture initialization failure",
					},
				})
				continue
			}
			protocolVersion := 1
			if len(os.Args) > 1 && os.Args[1] == "unsupported-protocol" {
				protocolVersion = 2
			}
			encode(encoder, message{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result: json.RawMessage(
					fmt.Sprintf(
						`{"protocolVersion":%d,"agentCapabilities":{},"agentInfo":{"name":"fixture","version":"dev"}}`,
						protocolVersion,
					),
				),
			})
		case "authenticate", "logout", "session/load", "session/resume", "session/close",
			"session/delete", "session/set_mode", "session/set_config_option":
			encode(encoder, message{JSONRPC: "2.0", ID: msg.ID, Result: json.RawMessage(`{}`)})
		case "session/new":
			encode(encoder, message{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  json.RawMessage(`{"sessionId":"fixture-session"}`),
			})
		case "session/list":
			encode(encoder, message{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  json.RawMessage(`{"sessions":[]}`),
			})
		case "session/cancel":
		case "session/prompt":
			runACPCallbacks(reader, encoder)
			encode(encoder, message{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  json.RawMessage(`{"stopReason":"end_turn"}`),
			})
		case "_fixture/echo":
			encode(encoder, message{JSONRPC: "2.0", ID: msg.ID, Result: msg.Params})
		case "_fixture/notify":
		case "echo":
			encode(encoder, message{JSONRPC: "2.0", ID: msg.ID, Result: msg.Params})
		case "environment":
			value, present := os.LookupEnv("DURABLE_ACP_TRANSPORT_TEST_ENV")
			result, _ := json.Marshal(map[string]any{"present": present, "value": value})
			encode(encoder, message{JSONRPC: "2.0", ID: msg.ID, Result: result})
		case "fail":
			encode(encoder, message{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Error: &rpcError{
					Code:    -32000,
					Message: "fixture failure",
					Data:    json.RawMessage(`{"retryable":false}`),
				},
			})
		case "ping":
			encode(encoder, message{JSONRPC: "2.0", Method: "observed/ping", Params: msg.Params})
		case "request":
			encode(encoder, message{JSONRPC: "2.0", ID: json.RawMessage("42"), Method: "client/answer", Params: json.RawMessage(`{"question":"continue?"}`)})
			responseLine, readErr := reader.ReadBytes('\n')
			if readErr != nil {
				os.Exit(2)
			}
			var response message
			if json.Unmarshal(responseLine, &response) != nil || string(response.ID) != "42" {
				os.Exit(3)
			}
			encode(encoder, message{JSONRPC: "2.0", ID: msg.ID, Result: response.Result})
		case "wait-cancel":
			cancelLine, readErr := reader.ReadBytes('\n')
			if readErr != nil {
				os.Exit(8)
			}
			var cancel message
			if json.Unmarshal(cancelLine, &cancel) != nil || cancel.Method != "$/cancel_request" {
				os.Exit(9)
			}
		case "request-cancel":
			encode(encoder, message{
				JSONRPC: "2.0",
				ID:      json.RawMessage(`43`),
				Method:  "client/wait",
			})
			encode(encoder, message{
				JSONRPC: "2.0",
				Method:  "$/cancel_request",
				Params:  json.RawMessage(`{"requestId":43}`),
			})
			responseLine, readErr := reader.ReadBytes('\n')
			if readErr != nil {
				os.Exit(10)
			}
			var response message
			if json.Unmarshal(responseLine, &response) != nil ||
				string(response.ID) != "43" ||
				response.Error == nil ||
				response.Error.Code != -32800 {
				os.Exit(11)
			}
			encode(encoder, message{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  json.RawMessage(`{"cancelled":true}`),
			})
		case "exit":
			if _, err := fmt.Fprintln(os.Stderr, "fixture failed"); err != nil {
				os.Exit(4)
			}
			os.Exit(7)
		case "large-stderr":
			payload := make([]byte, 9*1024-len("tail\n"))
			for i := range payload {
				payload[i] = 'x'
			}
			if _, err := os.Stderr.Write(append(payload, []byte("tail\n")...)); err != nil {
				os.Exit(5)
			}
			os.Exit(8)
		}
	}
}

func runACPCallbacks(reader *bufio.Reader, encoder *json.Encoder) {
	encode(encoder, message{
		JSONRPC: "2.0",
		Method:  "session/update",
		Params: json.RawMessage(
			`{"sessionId":"fixture-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}}`,
		),
	})
	requestClient(reader, encoder, 100, "session/request_permission", json.RawMessage(
		`{"sessionId":"fixture-session","toolCall":{"toolCallId":"tool-1","title":"Test"},"options":[]}`,
	))
	requestClient(reader, encoder, 101, "fs/read_text_file", json.RawMessage(
		`{"sessionId":"fixture-session","path":"/tmp/input.txt"}`,
	))
	requestClient(reader, encoder, 102, "fs/write_text_file", json.RawMessage(
		`{"sessionId":"fixture-session","path":"/tmp/output.txt","content":"hello"}`,
	))
	requestClient(reader, encoder, 103, "terminal/create", json.RawMessage(
		`{"sessionId":"fixture-session","command":"echo","args":["hello"],"env":[]}`,
	))
	requestClient(reader, encoder, 104, "terminal/output", json.RawMessage(
		`{"sessionId":"fixture-session","terminalId":"terminal-1"}`,
	))
	requestClient(reader, encoder, 105, "terminal/wait_for_exit", json.RawMessage(
		`{"sessionId":"fixture-session","terminalId":"terminal-1"}`,
	))
	requestClient(reader, encoder, 106, "terminal/kill", json.RawMessage(
		`{"sessionId":"fixture-session","terminalId":"terminal-1"}`,
	))
	requestClient(reader, encoder, 107, "terminal/release", json.RawMessage(
		`{"sessionId":"fixture-session","terminalId":"terminal-1"}`,
	))
	requestClient(reader, encoder, 108, "elicitation/create", json.RawMessage(
		`{"mode":"url","elicitationId":"elicit-1","message":"Authenticate","url":"https://example.com"}`,
	))
	encode(encoder, message{
		JSONRPC: "2.0",
		Method:  "elicitation/complete",
		Params:  json.RawMessage(`{"elicitationId":"elicit-1"}`),
	})
	requestClient(reader, encoder, 109, "_fixture/request", json.RawMessage(`{"value":true}`))
	encode(encoder, message{
		JSONRPC: "2.0",
		Method:  "_fixture/notification",
		Params:  json.RawMessage(`{"value":true}`),
	})
}

func requestClient(
	reader *bufio.Reader,
	encoder *json.Encoder,
	id int,
	method string,
	params json.RawMessage,
) {
	rawID := json.RawMessage(fmt.Sprintf("%d", id))
	encode(encoder, message{
		JSONRPC: "2.0",
		ID:      rawID,
		Method:  method,
		Params:  params,
	})
	responseLine, err := reader.ReadBytes('\n')
	if err != nil {
		os.Exit(12)
	}
	var response message
	if json.Unmarshal(responseLine, &response) != nil || string(response.ID) != string(rawID) || response.Error != nil {
		os.Exit(13)
	}
}

func encode(encoder *json.Encoder, msg message) {
	if err := encoder.Encode(msg); err != nil {
		os.Exit(6)
	}
}
