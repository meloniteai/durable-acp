package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCodexSourceHomePrefersExplicitConfiguration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real ACP runner requires bash")
	}
	runHelper(t, `
[ "$(realacp_codex_source_home "$2" "$3")" = "$2" ]
[ "$(realacp_codex_source_home "" "$3")" = "$3/.codex" ]
`, "/configured/codex", "/users/fixture", "unused")
}

func TestConfigureCodexWorkerIsolatesAuthenticationAndPinsExecutable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real ACP runner requires bash")
	}
	source := t.TempDir()
	worker := filepath.Join(t.TempDir(), "codex")
	wantAuth := []byte(`{"token":"fixture"}`)
	if err := os.WriteFile(filepath.Join(source, "auth.json"), wantAuth, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "config.toml"), []byte("personal = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	runHelper(t, `
realacp_configure_codex_worker vanilla "$2" "$3" "$4"
[ "$CODEX_HOME" = "$3" ]
[ "$CODEX_PATH" = "$4" ]
`, source, worker, "/fixture/codex")

	// #nosec G304 -- The test reads a fixed file below its temporary worker directory.
	gotAuth, err := os.ReadFile(filepath.Join(worker, "auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotAuth) != string(wantAuth) {
		t.Fatalf("worker auth = %q, want %q", gotAuth, wantAuth)
	}
	info, err := os.Stat(filepath.Join(worker, "auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("worker auth mode = %o, want 600", got)
	}
	if _, err := os.Stat(filepath.Join(worker, "config.toml")); !os.IsNotExist(err) {
		t.Fatalf("personal config copied into worker: %v", err)
	}
}

func TestConfigureCodexWorkerLeavesCredentialsOutOfOpenRouter(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real ACP runner requires bash")
	}
	source := t.TempDir()
	worker := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(filepath.Join(source, "auth.json"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}

	runHelper(t, `realacp_configure_codex_worker openrouter "$2" "$3" "$4"`, source, worker, "/fixture/codex")
	if _, err := os.Stat(filepath.Join(worker, "auth.json")); !os.IsNotExist(err) {
		t.Fatalf("OpenRouter worker auth stat = %v, want not exist", err)
	}
}

func TestScrubCodexAuthenticationFromRetainedArtifacts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real ACP runner requires bash")
	}
	runRoot := t.TempDir()
	auth := filepath.Join(runRoot, "workers", "0", "codex", "auth.json")
	if err := os.MkdirAll(filepath.Dir(auth), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(auth, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}

	runHelper(t, `realacp_scrub_codex_auth "$2"`, runRoot, "unused", "unused")
	if _, err := os.Stat(auth); !os.IsNotExist(err) {
		t.Fatalf("retained auth stat = %v, want not exist", err)
	}
}

func TestConfigureOpenRouterUsesOneModelForCodexAndClaude(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real ACP runner requires bash")
	}
	runHelper(t, `
export OPENROUTER_API_KEY=fixture-key
unset DURABLE_ACP_REAL_MODEL DURABLE_ACP_REAL_CODEX_MODEL DURABLE_ACP_REAL_CLAUDE_MODEL DURABLE_ACP_REAL_REASONING
realacp_configure_openrouter deepseek/deepseek-v4-flash
[ "$DURABLE_ACP_REAL_MODEL" = deepseek/deepseek-v4-flash ]
[ "$DURABLE_ACP_REAL_CODEX_MODEL" = deepseek/deepseek-v4-flash ]
[ "$DURABLE_ACP_REAL_CLAUDE_MODEL" = opus ]
[ "$DURABLE_ACP_REAL_REASONING" = low ]
[ "$ANTHROPIC_BASE_URL" = https://openrouter.ai/api ]
[ "$ANTHROPIC_AUTH_TOKEN" = "$OPENROUTER_API_KEY" ]
[ -z "$ANTHROPIC_API_KEY" ]
[ "$ANTHROPIC_MODEL" = deepseek/deepseek-v4-flash ]
[ "$ANTHROPIC_DEFAULT_OPUS_MODEL" = deepseek/deepseek-v4-flash ]
[ "$ANTHROPIC_DEFAULT_SONNET_MODEL" = deepseek/deepseek-v4-flash ]
[ "$ANTHROPIC_DEFAULT_HAIKU_MODEL" = deepseek/deepseek-v4-flash ]
[ "$CLAUDE_MODEL_CONFIG" = '{"availableModels":["deepseek/deepseek-v4-flash"]}' ]
[ "$CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC" = 1 ]
`, "unused", "unused", "unused")
}

func TestCodexOpenRouterTemplateUsesSharedKeyAndRetries(t *testing.T) {
	raw, err := os.ReadFile("realacp-openrouter.codex.toml")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`env_key = "OPENROUTER_API_KEY"`,
		`wire_api = "responses"`,
		`request_max_retries = 2`,
		`stream_max_retries = 2`,
	} {
		if !strings.Contains(string(raw), expected) {
			t.Errorf("Codex OpenRouter template missing %q", expected)
		}
	}
}

func runHelper(t *testing.T, body, first, second, third string) {
	t.Helper()
	// #nosec G204 -- The test invokes bash with test-owned paths and fixed helper bodies.
	command := exec.CommandContext(t.Context(), "bash", "-c", `source "$1"`+"\n"+body, "bash", "realacp-helpers.sh", first, second, third)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run real ACP helper: %v\n%s", err, output)
	}
}
