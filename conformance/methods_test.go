package conformance_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/meloniteai/durable-acp/acp"
)

func TestStableV1MethodCoverage(t *testing.T) {
	t.Parallel()

	expected := map[string]acp.Method{
		"$/cancel_request":           {Name: "$/cancel_request", Receiver: acp.PeerEither, Kind: acp.MessageNotification},
		"authenticate":               {Name: "authenticate", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"elicitation/complete":       {Name: "elicitation/complete", Receiver: acp.PeerClient, Kind: acp.MessageNotification},
		"elicitation/create":         {Name: "elicitation/create", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"fs/read_text_file":          {Name: "fs/read_text_file", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"fs/write_text_file":         {Name: "fs/write_text_file", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"initialize":                 {Name: "initialize", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"logout":                     {Name: "logout", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/cancel":             {Name: "session/cancel", Receiver: acp.PeerAgent, Kind: acp.MessageNotification},
		"session/close":              {Name: "session/close", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/delete":             {Name: "session/delete", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/list":               {Name: "session/list", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/load":               {Name: "session/load", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/new":                {Name: "session/new", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/prompt":             {Name: "session/prompt", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/request_permission": {Name: "session/request_permission", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"session/resume":             {Name: "session/resume", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/set_config_option":  {Name: "session/set_config_option", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/set_mode":           {Name: "session/set_mode", Receiver: acp.PeerAgent, Kind: acp.MessageRequest},
		"session/update":             {Name: "session/update", Receiver: acp.PeerClient, Kind: acp.MessageNotification},
		"terminal/create":            {Name: "terminal/create", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"terminal/kill":              {Name: "terminal/kill", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"terminal/output":            {Name: "terminal/output", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"terminal/release":           {Name: "terminal/release", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
		"terminal/wait_for_exit":     {Name: "terminal/wait_for_exit", Receiver: acp.PeerClient, Kind: acp.MessageRequest},
	}

	actual := make(map[string]acp.Method)
	for _, method := range acp.Methods() {
		require.NotContains(t, actual, method.Name)
		actual[method.Name] = method
	}
	assert.Equal(t, expected, actual)
	assert.Equal(t, acp.ProtocolVersion(1), acp.ProtocolVersion(acp.ProtocolVersionNumber))
	assert.Equal(t, "v1", acp.SchemaVersion)
	assert.Equal(t, "4544546a94bc63a9719fa5ba84583e6726c7bd09", acp.SchemaRevision)
}

func TestMethodsReturnsCopy(t *testing.T) {
	t.Parallel()

	first := acp.Methods()
	first[0].Name = "mutated"
	assert.NotEqual(t, first, acp.Methods())
}
