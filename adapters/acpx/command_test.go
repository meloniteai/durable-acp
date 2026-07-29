package acpx

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentEnvironmentSanitizesPathAndCredentials(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("OPENAI_API_KEY", "secret")
	pathEnv := strings.Join([]string{
		"/usr/bin",
		filepath.Join(home, "Downloads", "tools"),
		filepath.Join(home, ".local", "bin"),
		filepath.Join(home, "Documents", "tools"),
		filepath.Join(home, "Desktop", "bin"),
	}, string(os.PathListSeparator))

	environment := AgentEnvironment(pathEnv, "OPENAI_API_KEY")
	path := environmentValue(environment, "PATH")
	if path == "" {
		t.Fatal("PATH was not set")
	}
	for _, blocked := range []string{"Downloads", "Documents", "Desktop"} {
		if strings.Contains(path, blocked) {
			t.Fatalf("PATH %q contains blocked %s", path, blocked)
		}
	}
	for _, allowed := range []string{"/usr/bin", filepath.Join(home, ".local", "bin")} {
		if !strings.Contains(path, allowed) {
			t.Fatalf("PATH %q is missing %q", path, allowed)
		}
	}
	if value := environmentValue(environment, "OPENAI_API_KEY"); value != "" {
		t.Fatalf("OPENAI_API_KEY was retained as %q", value)
	}
}

func TestResolveSkipsProtectedPathEntries(t *testing.T) {
	home := t.TempDir()
	blocked := filepath.Join(home, "Downloads", "bin")
	allowed := t.TempDir()
	for _, directory := range []string{blocked, allowed} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		// #nosec G306 -- the test needs a runnable fixture.
		if err := os.WriteFile(filepath.Join(directory, "agent"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("HOME", home)
	t.Setenv("PATH", strings.Join([]string{blocked, allowed}, string(os.PathListSeparator)))
	t.Setenv("SHELL", filepath.Join(t.TempDir(), "missing-shell"))

	resolved, err := Resolve(context.Background(), "agent")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(allowed, "agent"); resolved.Path != want {
		t.Fatalf("path = %q, want %q", resolved.Path, want)
	}
	if strings.Contains(resolved.PathEnv, blocked) {
		t.Fatalf("child PATH %q contains blocked directory %q", resolved.PathEnv, blocked)
	}
}

func TestResolveUsesShellPath(t *testing.T) {
	bin := t.TempDir()
	tool := filepath.Join(bin, "agent")
	// #nosec G306 -- the test needs a runnable fixture.
	if err := os.WriteFile(tool, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	shell := filepath.Join(t.TempDir(), "shell")
	// #nosec G306 -- the test needs a runnable shell fixture.
	if err := os.WriteFile(shell, []byte("#!/bin/sh\nprintf '"+shellPathMarker+"%s\\n' \"$DURABLE_ACP_TEST_PATH\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("SHELL", shell)
	t.Setenv("DURABLE_ACP_TEST_PATH", bin)

	resolved, err := Resolve(context.Background(), "agent")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Path != tool {
		t.Fatalf("path = %q, want %q", resolved.Path, tool)
	}
	if !strings.Contains(resolved.PathEnv, bin) {
		t.Fatalf("child PATH = %q, want %q", resolved.PathEnv, bin)
	}
}

func TestFallbackPathAndSiblingExecutable(t *testing.T) {
	if path := fallbackPathEnv(); !strings.Contains(path, "/usr/bin") {
		t.Fatalf("fallback PATH = %q", path)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(filepath.Dir(executable), "durable-acp-sibling-fixture")
	// #nosec G306 -- the test needs a runnable sibling fixture.
	if err := os.WriteFile(fixture, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(fixture) })
	if got := SiblingExecutable("durable-acp-sibling-fixture"); got != fixture {
		t.Fatalf("sibling = %q, want %q", got, fixture)
	}
}

func environmentValue(environment []string, key string) string {
	for _, item := range environment {
		name, value, ok := strings.Cut(item, "=")
		if ok && name == key {
			return value
		}
	}
	return ""
}
