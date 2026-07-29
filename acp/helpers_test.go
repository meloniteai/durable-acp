package acp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContentHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		block ContentBlock
		want  string
	}{
		{name: "text", block: TextBlock("hello"), want: `{"type":"text","text":"hello"}`},
		{
			name:  "image",
			block: ImageBlock("aW1hZ2U=", "image/png"),
			want:  `{"type":"image","data":"aW1hZ2U=","mimeType":"image/png"}`,
		},
		{
			name:  "audio",
			block: AudioBlock("YXVkaW8=", "audio/wav"),
			want:  `{"type":"audio","data":"YXVkaW8=","mimeType":"audio/wav"}`,
		},
		{
			name:  "resource link",
			block: ResourceLinkBlock("README", "file:///README.md"),
			want:  `{"type":"resource_link","name":"README","uri":"file:///README.md"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(test.block)
			require.NoError(t, err)
			assert.JSONEq(t, test.want, string(raw))
		})
	}

	resource := EmbeddedResourceResource{
		TextResourceContents: &TextResourceContents{
			Text: "body",
			Uri:  "file:///README.md",
		},
	}
	raw, err := json.Marshal(ResourceBlock(resource))
	require.NoError(t, err)
	assert.JSONEq(
		t,
		`{"type":"resource","resource":{"uri":"file:///README.md","text":"body"}}`,
		string(raw),
	)
	assert.Equal(t, "value", *Ptr("value"))
}

func TestMethodsReturnsCopy(t *testing.T) {
	t.Parallel()

	first := Methods()
	require.NotEmpty(t, first)
	first[0].Name = "mutated"
	assert.NotEqual(t, first, Methods())
}
