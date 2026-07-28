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
	Code    int    `json:"code"`
	Message string `json:"message"`
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
		case "echo":
			encode(encoder, message{JSONRPC: "2.0", ID: msg.ID, Result: msg.Params})
		case "fail":
			encode(encoder, message{JSONRPC: "2.0", ID: msg.ID, Error: &rpcError{Code: -32000, Message: "fixture failure"}})
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

func encode(encoder *json.Encoder, msg message) {
	if err := encoder.Encode(msg); err != nil {
		os.Exit(6)
	}
}
