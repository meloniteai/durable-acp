package worktree

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestManagerCreateReopenEnsureAndRemove(t *testing.T) {
	t.Parallel()

	source := testRepo(t)
	manager, err := NewManager(Config{Root: filepath.Join(t.TempDir(), "worktrees"), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), CreateRequest{ID: "session-1", Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if created.Branch != "agent/session-1" || created.Path == "" || created.BaseSHA == "" {
		t.Fatalf("created = %+v", created)
	}
	if _, err := os.Stat(created.Path); err != nil {
		t.Fatalf("created worktree unavailable: %v", err)
	}

	reopened, err := manager.Reopen(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Path != created.Path || reopened.Branch != created.Branch {
		t.Fatalf("reopened = %+v, want %+v", reopened, created)
	}
	ensured, err := manager.EnsureBranch(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	if ensured.Branch != created.Branch {
		t.Fatalf("ensured branch = %q, want %q", ensured.Branch, created.Branch)
	}

	if err := manager.Remove(context.Background(), created, RemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(created.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed worktree stat error = %v, want not exist", err)
	}
	if err := git(source, "show-ref", "--verify", "--quiet", "refs/heads/"+created.Branch); err == nil {
		t.Fatalf("managed branch %q still exists", created.Branch)
	}
}

func TestManagerRejectsForeignRemoval(t *testing.T) {
	t.Parallel()

	source := testRepo(t)
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	err = manager.Remove(context.Background(), Session{Source: source, Path: source, Branch: "main"}, RemoveOptions{Force: true})
	if err == nil {
		t.Fatal("Remove succeeded for foreign worktree")
	}
}

func TestManagerRejectsExistingTarget(t *testing.T) {
	t.Parallel()

	source := testRepo(t)
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), CreateRequest{ID: "session-1", Source: source}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), CreateRequest{ID: "session-1", Source: source}); err == nil {
		t.Fatal("Create succeeded for an existing target")
	}
}

func TestManagerRejectsUnsafeSessionID(t *testing.T) {
	t.Parallel()

	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), CreateRequest{ID: ".", Source: testRepo(t)}); err == nil {
		t.Fatal("Create succeeded for unsafe session ID")
	}
}

func TestManagerRemovesBranchAfterExternalWorktreeRemoval(t *testing.T) {
	t.Parallel()

	source := testRepo(t)
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), CreateRequest{ID: "session-1", Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := git(source, "worktree", "remove", "--force", created.Path); err != nil {
		t.Fatal(err)
	}
	if err := manager.Remove(context.Background(), created, RemoveOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	if err := git(source, "show-ref", "--verify", "--quiet", "refs/heads/"+created.Branch); err == nil {
		t.Fatalf("managed branch %q still exists", created.Branch)
	}
}

//nolint:govet // Tests use scoped assertions for clearer failure locations.
func TestManagerRepairChecksOutOwnedBranch(t *testing.T) {
	source := testRepo(t)
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), CreateRequest{ID: "session-1", Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := git(created.Path, "checkout", "-b", "other"); err != nil {
		t.Fatal(err)
	}
	repaired, err := manager.Repair(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Branch != created.Branch || repaired.ID != created.ID || repaired.Source != created.Source {
		t.Fatalf("repaired = %#v", repaired)
	}
	if err := manager.Remove(context.Background(), created, RemoveOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureBranchInExistingWorkspace(t *testing.T) {
	source := testRepo(t)
	wantPath, err := filepath.EvalSymlinks(source)
	if err != nil {
		t.Fatal(err)
	}
	created, err := EnsureBranch(context.Background(), source, "agent/session-1")
	if err != nil {
		t.Fatal(err)
	}
	if created.Path != wantPath || created.Branch != "agent/session-1" || created.BaseSHA == "" {
		t.Fatalf("created = %#v", created)
	}
	if _, err := EnsureBranch(context.Background(), source, "agent/session-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureBranch(context.Background(), source, ""); err == nil {
		t.Fatal("EnsureBranch accepted an empty branch")
	}
}

func TestManagerValidationAndHelpers(t *testing.T) {
	if _, err := NewManager(Config{}); err == nil {
		t.Fatal("NewManager accepted empty root")
	}
	if _, err := NewManager(Config{Root: t.TempDir()}); err == nil {
		t.Fatal("NewManager accepted empty prefix")
	}
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "/agent/"})
	if err != nil {
		t.Fatal(err)
	}
	if manager.Root() == "" || (*Manager)(nil).Root() != "" {
		t.Fatal("Root did not handle managers correctly")
	}
	if _, err := manager.Inspect(context.Background(), t.TempDir()); err == nil {
		t.Fatal("Inspect accepted non-repository")
	}
	if _, err := manager.Reopen(context.Background(), Session{}); err == nil {
		t.Fatal("Reopen accepted empty session")
	}
	if _, err := manager.Repair(context.Background(), Session{}); err == nil {
		t.Fatal("Repair accepted empty session")
	}
	if _, err := manager.EnsureBranch(context.Background(), Session{Path: t.TempDir(), Branch: "foreign"}); err == nil {
		t.Fatal("EnsureBranch accepted foreign session")
	}
	if err := manager.Prune(context.Background(), t.TempDir()); err == nil {
		t.Fatal("Prune accepted non-repository")
	}
	if err := (*Manager)(nil).Remove(context.Background(), Session{}, RemoveOptions{}); err == nil {
		t.Fatal("nil Remove succeeded")
	}

	for raw, want := range map[string]string{
		"git@github.com:example/repo.git": "example/repo",
		"not-a-remote":                    "",
	} {
		if got := remoteKey(raw); got != want {
			t.Fatalf("remoteKey(%q) = %q, want %q", raw, got, want)
		}
	}
	if got := remoteKey(" https://github.com/example/repo.git/ "); got != "example/repo" {
		t.Fatalf("trimmed remote key = %q", got)
	}
	for raw, want := range map[string]string{"hello world": "hello-world", "..": "", "": "", "a/b": "a-b"} {
		if got := safePart(raw); got != want {
			t.Fatalf("safePart(%q) = %q, want %q", raw, got, want)
		}
	}
	if !within("/tmp/root", "/tmp/root/a") || within("/tmp/root", "/tmp/other") {
		t.Fatal("within returned an unsafe result")
	}
}

func TestManagerUsesRemoteRepositoryKeyAndRejectsForeignReopen(t *testing.T) {
	source := testRepo(t)
	if err := git(source, "remote", "add", "origin", "git@github.com:owner/repo.git"); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), CreateRequest{ID: "session-1", Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(filepath.Dir(created.Path)) != "repo" {
		t.Fatalf("target path = %q", created.Path)
	}
	foreign := created
	foreign.Path = source
	if _, err := manager.Reopen(context.Background(), foreign); err == nil {
		t.Fatal("Reopen accepted a foreign path")
	}
	if err := manager.Remove(context.Background(), created, RemoveOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestManagerUsesHostRepositoryKey(t *testing.T) {
	source := testRepo(t)
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), CreateRequest{
		ID:            "session-1",
		Source:        source,
		RepositoryKey: "acme/widgets",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := filepath.Dir(created.Path), filepath.Join(manager.Root(), "acme-widgets"); got != want {
		t.Fatalf("worktree parent = %q, want %q", got, want)
	}
	if err := manager.Remove(context.Background(), created, RemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), CreateRequest{ID: "second", Source: source, RepositoryKey: ".."}); err == nil {
		t.Fatal("Create accepted an invalid repository key")
	}
}

func TestManagerInternalGitHelpers(t *testing.T) {
	source := testRepo(t)
	manager, err := NewManager(Config{Root: t.TempDir(), BranchPrefix: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (*Manager)(nil).Create(context.Background(), CreateRequest{}); err == nil {
		t.Fatal("nil manager created a worktree")
	}
	if _, err := manager.Create(context.Background(), CreateRequest{Source: source}); err == nil {
		t.Fatal("Create accepted empty session ID")
	}
	if _, err := manager.Create(context.Background(), CreateRequest{ID: "s", Source: t.TempDir()}); err == nil {
		t.Fatal("Create accepted non-repository source")
	}
	if got := manager.branch("s"); got != "agent/s" || !manager.ownsBranch("agent/s") || manager.ownsBranch("other/s") {
		t.Fatal("branch ownership helpers returned an unexpected result")
	}
	if root, err := manager.repoRoot(context.Background(), source); err != nil || root == "" {
		t.Fatalf("repoRoot = %q, %v", root, err)
	}
	if key, err := manager.repoKey(context.Background(), source); err != nil || key == "" {
		t.Fatalf("repoKey = %q, %v", key, err)
	}
	if !manager.referenceExists(context.Background(), source, "refs/heads/main") || manager.referenceExists(context.Background(), source, "refs/heads/missing") {
		t.Fatal("reference existence was incorrect")
	}
	if _, err := manager.gitOutput(context.Background(), source, "rev-parse", "HEAD"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.gitOutput(context.Background(), source, "rev-parse", "does-not-exist"); err == nil {
		t.Fatal("gitOutput accepted invalid revision")
	}
	if err := manager.Prune(context.Background(), source); err != nil {
		t.Fatal(err)
	}
}

func testRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := git(dir, "init", "--initial-branch=main"); err != nil {
		t.Fatal(err)
	}
	if err := git(dir, "config", "user.email", "test@example.com"); err != nil {
		t.Fatal(err)
	}
	if err := git(dir, "config", "user.name", "Test User"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := git(dir, "add", "README.md"); err != nil {
		t.Fatal(err)
	}
	if err := git(dir, "commit", "-m", "initial"); err != nil {
		t.Fatal(err)
	}
	return dir
}

func git(directory string, args ...string) error {
	// #nosec G204 -- Test helper invokes the fixed Git binary in a temp repository.
	command := exec.CommandContext(context.Background(), "git", append([]string{"-C", directory}, args...)...)
	return command.Run()
}
