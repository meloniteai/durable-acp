package acpx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var errCommandNotFound = errors.New("executable not found")

const shellPathMarker = "__DURABLE_ACP_PATH__"

// Command is an executable resolved from an interactive shell's PATH. PathEnv
// is the sanitized PATH the child process should receive so its own helper
// processes resolve consistently with the adapter.
type Command struct {
	Path    string
	PathEnv string
}

var shellPathCache struct {
	sync.Mutex
	key     string
	pathEnv string
	ok      bool
	loading bool
	wait    chan struct{}
}

// Resolve finds command using a safe merge of the process PATH, the login
// shell PATH, and conventional installation directories. It never searches
// Desktop, Documents, or Downloads, which prevents a child agent from
// inheriting accidental executable locations in user-content folders.
func Resolve(ctx context.Context, command string) (Command, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return Command{}, errors.New("command is required")
	}
	if filepath.IsAbs(command) || strings.ContainsRune(command, filepath.Separator) {
		path := command
		if !filepath.IsAbs(path) {
			var err error
			path, err = filepath.Abs(path)
			if err != nil {
				return Command{}, err
			}
		}
		if !agentPathEntryAllowed(filepath.Dir(path)) {
			return Command{}, fmt.Errorf("%s: %w", command, errCommandNotFound)
		}
	}
	currentPath := filterPathEnv(os.Getenv("PATH"))
	runtimePath := runtimePathEnv(ctx, currentPath)
	if path, ok := findCommand(command, currentPath); ok {
		if runtimePath == "" {
			runtimePath = currentPath
		}
		return Command{Path: path, PathEnv: runtimePath}, nil
	}
	if path, ok := findCommand(command, runtimePath); ok {
		return Command{Path: path, PathEnv: runtimePath}, nil
	}
	return Command{}, fmt.Errorf("%s: %w", command, errCommandNotFound)
}

// AgentEnvironment returns a child environment with a sanitized PATH and
// selected credentials removed. It is appropriate for ACP sidecars that use
// their own authentication or runtime discovery.
func AgentEnvironment(pathEnv string, remove ...string) []string {
	safePath := filterPathEnv(pathEnv)
	if safePath == "" {
		safePath = fallbackPathEnv()
	}
	blocked := make(map[string]struct{}, len(remove))
	for _, key := range remove {
		blocked[key] = struct{}{}
	}
	environment := make([]string, 0, len(os.Environ())+1)
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		if ok {
			if _, found := blocked[key]; found {
				continue
			}
			if key == "PATH" {
				continue
			}
		}
		environment = append(environment, item)
	}
	return append(environment, "PATH="+safePath)
}

// SiblingExecutable finds an executable installed alongside the host binary.
// It supports a conventional unversioned name, Windows suffix, and a
// versioned suffix such as "codex-runtime-0.1.0".
func SiblingExecutable(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	executable, err := os.Executable()
	if err != nil {
		return ""
	}
	directory := filepath.Dir(executable)
	for _, candidate := range []string{filepath.Join(directory, prefix), filepath.Join(directory, prefix+".exe")} {
		if executableFile(candidate) {
			return candidate
		}
	}
	matches, err := filepath.Glob(filepath.Join(directory, prefix+"-*"))
	if err != nil {
		return ""
	}
	for _, candidate := range matches {
		if executableFile(candidate) {
			return candidate
		}
	}
	return ""
}

func runtimePathEnv(ctx context.Context, currentPath string) string {
	paths := []string{}
	if shellPath, ok := shellPathEnv(ctx); ok {
		paths = append(paths, filepath.SplitList(filterPathEnv(shellPath))...)
	}
	paths = append(paths, filepath.SplitList(currentPath)...)
	paths = append(paths, fallbackDirectories()...)
	return strings.Join(uniquePaths(paths), string(os.PathListSeparator))
}

func fallbackPathEnv() string {
	return strings.Join(uniquePaths(fallbackDirectories()), string(os.PathListSeparator))
}

func fallbackDirectories() []string {
	directories := []string{
		"/opt/homebrew/bin",
		"/usr/local/bin",
		"/home/linuxbrew/.linuxbrew/bin",
		"/usr/bin",
		"/bin",
		"/usr/sbin",
		"/sbin",
	}
	if home, err := os.UserHomeDir(); err == nil {
		directories = append(directories,
			filepath.Join(home, ".local", "bin"),
			filepath.Join(home, "bin"),
			filepath.Join(home, ".npm-global", "bin"),
			filepath.Join(home, ".bun", "bin"),
			filepath.Join(home, ".cargo", "bin"),
			filepath.Join(home, ".deno", "bin"),
			filepath.Join(home, ".claude", "local"),
		)
	}
	return directories
}

func shellPathEnv(ctx context.Context) (string, bool) {
	key := strings.Join([]string{os.Getenv("SHELL"), os.Getenv("HOME"), os.Getenv("PATH")}, "\x00")
	for {
		shellPathCache.Lock()
		if shellPathCache.key == key {
			pathEnv, ok := shellPathCache.pathEnv, shellPathCache.ok
			shellPathCache.Unlock()
			return pathEnv, ok
		}
		if shellPathCache.loading {
			wait := shellPathCache.wait
			shellPathCache.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return "", false
			}
		}
		shellPathCache.loading = true
		shellPathCache.wait = make(chan struct{})
		shellPathCache.Unlock()
		break
	}

	var pathEnv string
	var ok bool
	for _, shell := range shellCandidates() {
		for _, flag := range []string{"-lc", "-ic"} {
			pathEnv, ok = readShellPath(ctx, shell, flag)
			if ok {
				break
			}
		}
		if ok {
			break
		}
	}
	shellPathCache.Lock()
	if ctx.Err() == nil {
		shellPathCache.key, shellPathCache.pathEnv, shellPathCache.ok = key, pathEnv, ok
	}
	shellPathCache.loading = false
	close(shellPathCache.wait)
	shellPathCache.wait = nil
	shellPathCache.Unlock()
	return pathEnv, ok
}

func shellCandidates() []string {
	candidates := []string{}
	if shell := strings.TrimSpace(os.Getenv("SHELL")); shell != "" && executableFile(shell) {
		candidates = append(candidates, shell)
	}
	candidates = append(candidates, "/bin/zsh", "/bin/bash", "/bin/sh")
	return uniquePaths(candidates)
}

func readShellPath(ctx context.Context, shell, flag string) (string, bool) {
	shellContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// #nosec G204 -- shell is selected from validated executable candidates.
	command := exec.CommandContext(shellContext, shell, flag, `printf '`+shellPathMarker+`%s\n' "$PATH"`)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", false
	}
	for line := range strings.Lines(string(output)) {
		if pathEnv, found := strings.CutPrefix(strings.TrimSpace(line), shellPathMarker); found {
			return pathEnv, pathEnv != ""
		}
	}
	return "", false
}

func findCommand(command, pathEnv string) (string, bool) {
	if filepath.IsAbs(command) {
		if executableFile(command) {
			return filepath.Clean(command), true
		}
		return "", false
	}
	if strings.ContainsRune(command, filepath.Separator) {
		path, err := filepath.Abs(command)
		if err == nil && executableFile(path) {
			return filepath.Clean(path), true
		}
		return "", false
	}
	for _, directory := range filepath.SplitList(pathEnv) {
		if directory == "" {
			continue
		}
		path := filepath.Join(directory, command)
		if executableFile(path) {
			return path, true
		}
	}
	return "", false
}

func filterPathEnv(pathEnv string) string {
	directories := filepath.SplitList(pathEnv)
	kept := make([]string, 0, len(directories))
	for _, directory := range directories {
		if agentPathEntryAllowed(directory) {
			kept = append(kept, directory)
		}
	}
	return strings.Join(uniquePaths(kept), string(os.PathListSeparator))
}

func agentPathEntryAllowed(directory string) bool {
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return false
	}
	if suffix, found := strings.CutPrefix(directory, "~/"); found {
		if home, err := os.UserHomeDir(); err == nil {
			directory = filepath.Join(home, suffix)
		}
	}
	if !filepath.IsAbs(directory) {
		return true
	}
	directory = filepath.Clean(directory)
	home, err := os.UserHomeDir()
	if err != nil {
		return true
	}
	for _, name := range []string{"Desktop", "Documents", "Downloads"} {
		protected := filepath.Join(home, name)
		if directory == protected || strings.HasPrefix(directory, protected+string(os.PathSeparator)) {
			return false
		}
	}
	return true
}

func executableFile(path string) bool {
	// #nosec G703 -- this only reads executable metadata for an already-filtered candidate.
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

func uniquePaths(paths []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if _, found := seen[path]; found {
			continue
		}
		seen[path] = struct{}{}
		result = append(result, path)
	}
	return result
}
