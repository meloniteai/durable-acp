// Package worktree manages isolated Git worktrees owned by an embedding host.
package worktree

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Config controls where managed worktrees and branches are created.
type Config struct {
	Root         string
	BranchPrefix string
	GitCommand   string
}

// Manager creates and removes worktrees below one owned root.
type Manager struct {
	root         string
	branchPrefix string
	git          string
}

// Session identifies one managed worktree and its branch.
type Session struct {
	ID      string `json:"id"`
	Source  string `json:"source"`
	Path    string `json:"path"`
	Branch  string `json:"branch"`
	BaseSHA string `json:"base_sha"`
}

// CreateRequest identifies the source repository and session to isolate.
type CreateRequest struct {
	ID            string
	Source        string
	RepositoryKey string
}

// RemoveOptions controls destructive cleanup of an owned worktree.
type RemoveOptions struct {
	Force bool
}

// NewManager creates the owned worktree root when it does not yet exist.
func NewManager(config Config) (*Manager, error) {
	root := strings.TrimSpace(config.Root)
	if root == "" {
		return nil, errors.New("worktree: root is required")
	}
	prefix := strings.Trim(strings.TrimSpace(config.BranchPrefix), "/")
	if prefix == "" {
		return nil, errors.New("worktree: branch prefix is required")
	}
	git := strings.TrimSpace(config.GitCommand)
	if git == "" {
		git = "git"
	}
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("worktree: create root: %w", err)
	}
	// #nosec G302 -- Managed worktree roots are intentionally private to their owner.
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("worktree: secure root: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	return &Manager{root: root, branchPrefix: prefix, git: git}, nil
}

// Root returns the directory containing every worktree owned by this manager.
func (m *Manager) Root() string {
	if m == nil {
		return ""
	}
	return m.root
}

// Create creates an isolated worktree at Root/<repo>/<session> on a new
// managed branch. Any partial Git state created by a failed add is rolled back.
//
//nolint:govet // Scoped Git and filesystem errors preserve their operation context.
func (m *Manager) Create(ctx context.Context, request CreateRequest) (Session, error) {
	if m == nil {
		return Session{}, errors.New("worktree: nil manager")
	}
	id := safePart(request.ID)
	if id == "" {
		return Session{}, errors.New("worktree: session ID is required")
	}
	source, err := m.repoRoot(ctx, request.Source)
	if err != nil {
		return Session{}, err
	}
	repoKey := strings.TrimSpace(request.RepositoryKey)
	var repo string
	if repoKey != "" {
		repo = safePart(repoKey)
		if repo == "" {
			return Session{}, errors.New("worktree: repository key is invalid")
		}
	} else {
		repo, err = m.repoKey(ctx, source)
		if err != nil {
			return Session{}, err
		}
	}
	target := filepath.Join(m.root, repo, id)
	if !within(m.root, target) {
		return Session{}, errors.New("worktree: generated target escapes root")
	}
	if _, err := os.Stat(target); err == nil {
		return Session{}, fmt.Errorf("worktree: target %q already exists", target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Session{}, fmt.Errorf("worktree: inspect target: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return Session{}, fmt.Errorf("worktree: create target parent: %w", err)
	}

	branch := m.branch(id)
	branchExisted := m.referenceExists(ctx, source, "refs/heads/"+branch)
	if _, err := m.gitOutput(ctx, source, "worktree", "add", "--quiet", "-b", branch, target, "HEAD"); err != nil {
		m.rollback(ctx, source, target, branch, branchExisted)
		return Session{}, fmt.Errorf("worktree: add: %w", err)
	}
	info, err := m.Inspect(ctx, target)
	if err != nil {
		m.rollback(ctx, source, target, branch, branchExisted)
		return Session{}, err
	}
	info.ID = id
	info.Source = source
	info.Branch = branch
	return info, nil
}

// Inspect reads the current repository, branch, and commit of path.
func (m *Manager) Inspect(ctx context.Context, path string) (Session, error) {
	if m == nil {
		return Session{}, errors.New("worktree: nil manager")
	}
	root, err := m.repoRoot(ctx, path)
	if err != nil {
		return Session{}, err
	}
	branch, err := m.gitOutput(ctx, root, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return Session{}, fmt.Errorf("worktree: branch: %w", err)
	}
	sha, err := m.gitOutput(ctx, root, "rev-parse", "HEAD")
	if err != nil {
		return Session{}, fmt.Errorf("worktree: base SHA: %w", err)
	}
	return Session{Path: root, Branch: branch, BaseSHA: sha}, nil
}

// Reopen verifies that a persisted managed session still points to its branch.
func (m *Manager) Reopen(ctx context.Context, session Session) (Session, error) {
	if strings.TrimSpace(session.Path) == "" || strings.TrimSpace(session.Source) == "" || strings.TrimSpace(session.Branch) == "" {
		return Session{}, errors.New("worktree: session path, source, and branch are required")
	}
	if !within(m.root, session.Path) {
		return Session{}, fmt.Errorf("worktree: session path %q is not managed by this manager", session.Path)
	}
	info, err := m.Inspect(ctx, session.Path)
	if err != nil {
		return Session{}, err
	}
	if info.Branch != session.Branch {
		return Session{}, fmt.Errorf("worktree: %q is on branch %q, want %q", session.Path, info.Branch, session.Branch)
	}
	info.ID = session.ID
	info.Source = session.Source
	return info, nil
}

// Repair verifies a persisted worktree and restores its owned branch as the
// checked-out branch. It is safe to call after an interrupted host process;
// it never creates a worktree or touches a branch outside this manager's
// configured prefix.
func (m *Manager) Repair(ctx context.Context, session Session) (Session, error) {
	if m == nil {
		return Session{}, errors.New("worktree: nil manager")
	}
	if strings.TrimSpace(session.Path) == "" || strings.TrimSpace(session.Source) == "" || strings.TrimSpace(session.Branch) == "" {
		return Session{}, errors.New("worktree: session path, source, and branch are required")
	}
	if !within(m.root, session.Path) || !m.ownsBranch(session.Branch) {
		return Session{}, errors.New("worktree: session is not managed by this manager")
	}
	return m.EnsureBranch(ctx, session)
}

// EnsureBranch checks out the session branch, creating it only when it does
// not exist. It refuses paths or branches outside this manager's ownership.
func (m *Manager) EnsureBranch(ctx context.Context, session Session) (Session, error) {
	if m == nil {
		return Session{}, errors.New("worktree: nil manager")
	}
	if !within(m.root, session.Path) || !m.ownsBranch(session.Branch) {
		return Session{}, errors.New("worktree: session is not managed by this manager")
	}
	info, err := m.ensureBranch(ctx, session.Path, session.Branch)
	if err != nil {
		return Session{}, err
	}
	info.ID = session.ID
	info.Source = session.Source
	return info, nil
}

// EnsureBranch checks out branch in an existing Git workspace, creating it
// from the current HEAD only when it does not exist. It does not claim the
// workspace or remove any Git state.
func EnsureBranch(ctx context.Context, path, branch string) (Session, error) {
	manager := &Manager{git: "git"}
	return manager.ensureBranch(ctx, path, branch)
}

func (m *Manager) ensureBranch(ctx context.Context, path, branch string) (Session, error) {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return Session{}, errors.New("worktree: branch is required")
	}
	info, err := m.Inspect(ctx, path)
	if err != nil {
		return Session{}, err
	}
	if info.Branch != branch {
		if m.referenceExists(ctx, info.Path, "refs/heads/"+branch) {
			_, err = m.gitOutput(ctx, info.Path, "checkout", "--quiet", branch)
		} else {
			_, err = m.gitOutput(ctx, info.Path, "checkout", "--quiet", "-b", branch)
		}
		if err != nil {
			return Session{}, fmt.Errorf("worktree: checkout branch: %w", err)
		}
		info, err = m.Inspect(ctx, info.Path)
		if err != nil {
			return Session{}, err
		}
	}
	return info, nil
}

// Remove removes a worktree created by this manager and deletes its exact
// managed branch. A branch outside the configured prefix is never deleted.
func (m *Manager) Remove(ctx context.Context, session Session, options RemoveOptions) error {
	if m == nil {
		return errors.New("worktree: nil manager")
	}
	if strings.TrimSpace(session.Source) == "" || !within(m.root, session.Path) || !m.ownsBranch(session.Branch) {
		return errors.New("worktree: session is not managed by this manager")
	}
	args := []string{"worktree", "remove"}
	if options.Force {
		args = append(args, "--force")
	}
	args = append(args, session.Path)
	if _, err := m.gitOutput(ctx, session.Source, args...); err != nil {
		if _, statErr := os.Stat(session.Path); !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("worktree: remove: %w", err)
		}
		if pruneErr := m.Prune(ctx, session.Source); pruneErr != nil {
			return fmt.Errorf("worktree: remove missing worktree: %w", errors.Join(err, pruneErr))
		}
	}
	if m.referenceExists(ctx, session.Source, "refs/heads/"+session.Branch) {
		if _, err := m.gitOutput(ctx, session.Source, "branch", "-D", session.Branch); err != nil {
			return fmt.Errorf("worktree: delete branch: %w", err)
		}
	}
	return nil
}

// Prune removes stale Git worktree registrations from source. It never removes
// a live directory or branch.
func (m *Manager) Prune(ctx context.Context, source string) error {
	root, err := m.repoRoot(ctx, source)
	if err != nil {
		return err
	}
	if _, err := m.gitOutput(ctx, root, "worktree", "prune"); err != nil {
		return fmt.Errorf("worktree: prune: %w", err)
	}
	return nil
}

func (m *Manager) rollback(ctx context.Context, source, target, branch string, branchExisted bool) {
	_, _ = m.gitOutput(ctx, source, "worktree", "remove", "--force", target)
	if within(m.root, target) {
		_ = os.RemoveAll(target)
	}
	if !branchExisted && m.referenceExists(ctx, source, "refs/heads/"+branch) {
		_, _ = m.gitOutput(ctx, source, "branch", "-D", branch)
	}
}

func (m *Manager) branch(id string) string {
	return m.branchPrefix + "/" + id
}

func (m *Manager) ownsBranch(branch string) bool {
	return strings.HasPrefix(strings.TrimSpace(branch), m.branchPrefix+"/")
}

func (m *Manager) repoRoot(ctx context.Context, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("worktree: repository path is required")
	}
	root, err := m.gitOutput(ctx, path, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("worktree: resolve repository: %w", err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(root); resolveErr == nil {
		root = resolved
	}
	return filepath.Clean(root), nil
}

func (m *Manager) repoKey(ctx context.Context, source string) (string, error) {
	remote, err := m.gitOutput(ctx, source, "remote", "get-url", "origin")
	if err == nil {
		if key := remoteKey(remote); key != "" {
			return key, nil
		}
	}
	key := safePart(filepath.Base(source))
	if key == "" {
		return "", errors.New("worktree: derive repository key")
	}
	return key, nil
}

func (m *Manager) referenceExists(ctx context.Context, source, reference string) bool {
	// #nosec G204 -- exec uses the configured Git binary without shell interpolation.
	command := exec.CommandContext(ctx, m.git, "-C", source, "show-ref", "--verify", "--quiet", reference)
	return command.Run() == nil
}

func (m *Manager) gitOutput(ctx context.Context, directory string, args ...string) (string, error) {
	// #nosec G204 -- exec uses the configured Git binary without shell interpolation.
	command := exec.CommandContext(ctx, m.git, append([]string{"-C", directory}, args...)...)
	out, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func remoteKey(raw string) string {
	value := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(raw), "/"), ".git")
	if index := strings.Index(value, "://"); index >= 0 {
		value = value[index+3:]
	} else if index := strings.Index(value, ":"); index >= 0 && strings.Contains(value[:index], "@") {
		value = value[:index] + "/" + value[index+1:]
	}
	parts := strings.Split(value, "/")
	if len(parts) < 3 {
		return ""
	}
	owner := safePart(parts[len(parts)-2])
	repo := safePart(parts[len(parts)-1])
	if owner == "" || repo == "" {
		return ""
	}
	return filepath.Join(owner, repo)
}

func safePart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var out strings.Builder
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z', character >= '0' && character <= '9', character == '-', character == '_', character == '.':
			out.WriteRune(character)
		default:
			out.WriteByte('-')
		}
	}
	result := strings.Trim(out.String(), "-")
	if result == "." || result == ".." {
		return ""
	}
	return result
}

func within(root, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
