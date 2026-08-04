package conformance

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestPackageBoundaries(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	allowed := map[string][]string{
		"acp":                  nil,
		"transport":            nil,
		"client":               {"acp", "transport"},
		"host":                 nil,
		"session":              {"host"},
		"journal":              {"host"},
		"adapters/acpx":        {"acp", "client", "host"},
		"adapters/claude":      {"acp", "adapters/acpx", "host"},
		"adapters/codex":       {"acp", "adapters/acpx", "host"},
		"adapters/cursor":      {"acp", "adapters/acpx", "host"},
		"adapters/antigravity": {"adapters/acpx", "host"},
		"adapters":             {"adapters/acpx", "adapters/antigravity", "adapters/claude", "adapters/codex", "adapters/cursor", "host"},
		"runtime":              {"host", "journal", "session"},
		"worktree":             nil,
	}
	for packageName, dependencies := range allowed {
		t.Run(packageName, func(t *testing.T) {
			t.Parallel()
			checkPackageImports(t, root, packageName, dependencies)
		})
	}
}

func TestNoProductLeakage(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	forbidden := []string{
		"github.com/meloniteai/melonite/",
		"HarnessMode",
		"LoopResponder",
		"NativePlanGate",
		"PromptWeave",
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (entry.Name()[0] == '.' || entry.Name() == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		//nolint:gosec // WalkDir constrains path to this repository.
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, value := range forbidden {
			if strings.Contains(string(raw), value) {
				t.Errorf("%s contains product identifier %q", filepath.ToSlash(path), value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve conformance source path")
	}
	return filepath.Dir(filepath.Dir(file))
}

func checkPackageImports(t *testing.T, root, packageName string, allowed []string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(packageName)))
	if err != nil {
		t.Fatal(err)
	}
	modulePrefix := "github.com/meloniteai/durable-acp/"
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(root, filepath.FromSlash(packageName), entry.Name())
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		for _, spec := range file.Imports {
			importPath := strings.Trim(spec.Path.Value, `"`)
			if !strings.HasPrefix(importPath, modulePrefix) {
				continue
			}
			dependency := strings.TrimPrefix(importPath, modulePrefix)
			if !slices.Contains(allowed, dependency) {
				t.Errorf("%s imports forbidden internal package %q", entry.Name(), importPath)
			}
		}
	}
}
