package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	schemaRevision = "4544546a94bc63a9719fa5ba84583e6726c7bd09"
	generator      = "github.com/coder/acp-go-sdk/cmd/generate@v0.0.0-20260602142356-0845a3bb9edd"
)

var inputs = []schemaInput{
	{
		name:   "schema.json",
		digest: "7f1fba1561163729115247df75b67aeed02085115fbc7ef0131fb01d456c08f9",
	},
	{
		name:   "meta.json",
		digest: "061edb6efa8fb2aa2792459a86ec7268de5fe665bba48b2ffe7939df01481f88",
	},
}

var artifacts = map[string]string{
	"constants_gen.go": "schema_constants_gen.go",
	"helpers_gen.go":   "schema_helpers_gen.go",
	"types_gen.go":     "schema_types_gen.go",
}

type schemaInput struct {
	name   string
	digest string
}

func main() {
	out := flag.String("out", ".", "directory for generated Go files")
	flag.Parse()
	if err := generate(context.Background(), *out); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generate(ctx context.Context, out string) error {
	root, err := os.MkdirTemp("", "durable-acp-schema-")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(root) }()

	schemaDir := filepath.Join(root, "schema")
	generatedDir := filepath.Join(root, "generated")
	if err := os.MkdirAll(schemaDir, 0o700); err != nil {
		return fmt.Errorf("create schema directory: %w", err)
	}
	if err := os.MkdirAll(generatedDir, 0o700); err != nil {
		return fmt.Errorf("create generated directory: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for _, input := range inputs {
		url := fmt.Sprintf(
			"https://raw.githubusercontent.com/agentclientprotocol/agent-client-protocol/%s/schema/v1/%s",
			schemaRevision,
			input.name,
		)
		raw, err := download(ctx, client, url)
		if err != nil {
			return err
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != input.digest {
			return fmt.Errorf("%s checksum is %s, want %s", input.name, got, input.digest)
		}
		if err := os.WriteFile(filepath.Join(schemaDir, input.name), raw, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", input.name, err)
		}
	}

	//nolint:gosec // The executable and module version are pinned above.
	command := exec.CommandContext(
		ctx,
		"go",
		"run",
		generator,
		"-schema",
		schemaDir,
		"-out",
		generatedDir,
	)
	command.Dir = root
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("generate ACP schema: %w", err)
	}

	if err := os.MkdirAll(out, 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	for source, destination := range artifacts {
		//nolint:gosec // source is selected from the fixed artifact map above.
		raw, err := os.ReadFile(filepath.Join(generatedDir, source))
		if err != nil {
			return fmt.Errorf("read generated %s: %w", source, err)
		}
		raw, err = repairGeneratedSource(source, raw)
		if err != nil {
			return err
		}
		//nolint:gosec // Generated Go source is intentionally world-readable.
		if err := os.WriteFile(filepath.Join(out, destination), raw, 0o644); err != nil {
			return fmt.Errorf("write generated %s: %w", destination, err)
		}
	}
	return nil
}

func repairGeneratedSource(name string, raw []byte) ([]byte, error) {
	if name != "types_gen.go" {
		return raw, nil
	}
	broken := []byte(`func (v *CreateElicitationRequest) Validate() error {
	if v.Message == "" {
		return fmt.Errorf("message is required")
	}
	return nil
}`)
	fixed := []byte(`func (v *CreateElicitationRequest) Validate() error {
	if v == nil {
		return errors.New("elicitation request is required")
	}
	var message string
	switch {
	case v.Form != nil:
		message = v.Form.Message
	case v.Url != nil:
		message = v.Url.Message
	case v.Other != nil:
		message = v.Other.Message
	default:
		return errors.New("elicitation request variant is required")
	}
	if message == "" {
		return fmt.Errorf("message is required")
	}
	return nil
}`)
	if bytes.Count(raw, broken) != 1 {
		return nil, errors.New("generated elicitation validation no longer matches the expected source")
	}
	raw = bytes.Replace(raw, broken, fixed, 1)

	unreachable := []byte(`		return _b, nil
		var m map[string]any
		if json.Unmarshal(_b, &m) != nil {
			return []byte{}, errors.New("invalid variant payload")
		}
		return json.Marshal(m)
`)
	if bytes.Count(raw, unreachable) != 18 {
		return nil, errors.New("generated union marshaling no longer matches the expected source")
	}
	return bytes.ReplaceAll(raw, unreachable, []byte("\t\treturn _b, nil\n")), nil
}

func download(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create schema request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", url, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil, fmt.Errorf("download %s: %s", url, response.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", url, err)
	}
	if len(raw) == 0 {
		return nil, errors.New("downloaded schema is empty")
	}
	return raw, nil
}
