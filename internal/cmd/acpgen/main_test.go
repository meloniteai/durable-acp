package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepairGeneratedSource(t *testing.T) {
	t.Parallel()

	validation := `func (v *CreateElicitationRequest) Validate() error {
	if v.Message == "" {
		return fmt.Errorf("message is required")
	}
	return nil
}`
	unreachable := `		return _b, nil
		var m map[string]any
		if json.Unmarshal(_b, &m) != nil {
			return []byte{}, errors.New("invalid variant payload")
		}
		return json.Marshal(m)
`
	input := []byte(validation + "\n" + strings.Repeat(unreachable, 18))
	output, err := repairGeneratedSource("types_gen.go", input)
	require.NoError(t, err)
	assert.NotContains(t, string(output), "v.Message")
	assert.NotContains(t, string(output), "var m map[string]any")
	assert.Contains(t, string(output), "v.Form.Message")

	unchanged, err := repairGeneratedSource("constants_gen.go", []byte("source"))
	require.NoError(t, err)
	assert.Equal(t, []byte("source"), unchanged)

	_, err = repairGeneratedSource("types_gen.go", []byte("unexpected"))
	require.Error(t, err)
}

func TestDownload(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/schema":
			_, _ = writer.Write([]byte(`{"version":1}`))
		case "/empty":
		case "/missing":
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)

	client := server.Client()
	raw, err := download(context.Background(), client, server.URL+"/schema")
	require.NoError(t, err)
	assert.True(t, bytes.Equal([]byte(`{"version":1}`), raw))

	_, err = download(context.Background(), client, server.URL+"/empty")
	require.Error(t, err)
	_, err = download(context.Background(), client, server.URL+"/missing")
	require.Error(t, err)
}
