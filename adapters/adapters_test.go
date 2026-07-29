package adapters

import (
	"bytes"
	"io"
	"testing"

	"github.com/meloniteai/durable-acp/host"
)

func TestDefaultProvidesNativeBackends(t *testing.T) {
	var diagnostics bytes.Buffer
	set := Default(WithStderrFor(func(host.Backend) io.Writer { return &diagnostics }))
	if len(set) != 4 {
		t.Fatalf("adapter count = %d", len(set))
	}
	want := map[host.Backend]bool{"claude": true, "codex": true, "cursor": true, "antigravity": true}
	for _, adapter := range set {
		delete(want, adapter.Backend())
	}
	if len(want) != 0 {
		t.Fatalf("missing backends: %#v", want)
	}
}

func TestDefaultAcceptsNilOptions(t *testing.T) {
	if got := Default(nil); len(got) != 4 {
		t.Fatalf("adapter count = %d", len(got))
	}
}
