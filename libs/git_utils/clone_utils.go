package git_utils

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
)

func createTempDir() (string, error) {
	tempDir, err := os.MkdirTemp("", "repo")
	if err != nil {
		slog.Error("Failed to create temporary directory", "error", err)
		return "", err
	}
	return tempDir, nil
}

type action func(string) error

func CloneGitRepoAndDoAction(repoUrl string, branch string, commitHash string, token string, tokenUsername string, action action) error {
	dir, err := createTempDir()
	if err != nil {
		slog.Error("Failed to create temporary directory", "error", err)
		return err
	}
	defer func() {
		slog.Debug("Removing cloned directory", "directory", dir)
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("Failed to remove directory", "directory", dir, "error", err)
		}
	}()

	slog.Debug("Cloning git repository",
		"repoUrl", repoUrl,
		"branch", branch,
		"commitHash", commitHash,
		"directory", dir,
	)

	git := NewGitShellWithTokenAuth(dir, token, tokenUsername)
	if commitHash != "" {
		err = git.CloneCommit(repoUrl, commitHash)
	} else {
		err = git.Clone(repoUrl, branch)
	}
	if err != nil {
		slog.Error("Failed to clone repository",
			"repoUrl", repoUrl,
			"branch", branch,
			"error", err,
		)
		return err
	}

	err = action(dir)
	if err != nil {
		slog.Error("Error performing action on repository", "directory", dir, "error", err)
		return err
	}

	return nil
}

type GitAuth struct {
	Username      string
	Password      string // Can be either password or access token
	TokenUsername string // if set will replace x-access-token (needed for bitbucket which uses x-token-auth)
	Token         string // x-access-token
}

type GitShell struct {
	workDir       string
	parentContext context.Context
	timeout       time.Duration
	environment   []string
	auth          *GitAuth
}

func NewGitShell(workDir string, auth *GitAuth) *GitShell {
	env := os.Environ()

	// If authentication is provided, set up credential helper
	if auth != nil {
		// Add credential helper to avoid interactive password prompts
		env = append(env, "GIT_TERMINAL_PROMPT=0")
	}

	return &GitShell{
		workDir:       workDir,
		parentContext: context.Background(),
		timeout:       30 * time.Second,
		environment:   env,
		auth:          auth,
	}
}

func NewGitShellWithTokenAuth(workDir string, token string, tokenUsername string) *GitShell {
	auth := GitAuth{
		Username:      "x-access-token",
		Password:      "",
		TokenUsername: tokenUsername,
		Token:         token,
	}
	return NewGitShell(workDir, &auth)
}

// formatAuthURL injects credentials into the Git URL
func (g *GitShell) formatAuthURL(repoURL string) (string, error) {
	if g.auth == nil {
		return repoURL, nil
	}

	parsedURL, err := url.Parse(repoURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %v", err)
	}

	// Handle different auth types
	if g.auth.Token != "" {
		// X-Access-Token authentication
		tokenUsername := g.auth.TokenUsername
		if tokenUsername == "" {
			tokenUsername = "x-access-token"
		}
		parsedURL.User = url.UserPassword(tokenUsername, g.auth.Token)
	} else if g.auth.Username != "" {
		// Username/password or personal access token
		parsedURL.User = url.UserPassword(g.auth.Username, g.auth.Password)
	}

	return parsedURL.String(), nil
}

func (g *GitShell) runCommand(args ...string) (string, error) {
	output, err := g.runCommandRaw(args...)
	return strings.TrimSpace(output), err
}

func (g *GitShell) runCommandRaw(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(g.parentContext, g.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.WaitDelay = 2 * time.Second
	configureGitCommandCancellation(cmd)
	cmd.Dir = g.workDir
	cmd.Env = g.environment

	// Set up credential helper for HTTPS
	if g.auth != nil {
		cmd.Env = append(cmd.Env, "GIT_ASKPASS=echo")
		if g.auth.Token != "" {
			cmd.Env = append(cmd.Env, fmt.Sprintf("GIT_ACCESS_TOKEN=%s", g.auth.Token))
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		if stderr.Len() > 0 {
			return "", fmt.Errorf("git command failed: %v: %s", err, stderr.String())
		}
		return "", err
	}
	return stdout.String(), nil
}

func (g *GitShell) Checkout(branchOrCommit string) error {
	args := []string{"checkout"}
	args = append(args, branchOrCommit)
	_, err := g.runCommand(args...)
	return err
}

// CloneCommit fetches a full immutable object ID, independent of branch movement
// or deletion. It never substitutes the current tip when the object is missing.
func (g *GitShell) CloneCommit(repoURL, commitHash string) error {
	objectID, err := hex.DecodeString(commitHash)
	if err != nil || (len(objectID) != 20 && len(objectID) != 32) {
		return fmt.Errorf("pinned checkout requires a full commit object ID")
	}
	format := "sha1"
	if len(objectID) == 32 {
		format = "sha256"
	}
	if _, err := g.runCommand("init", "--object-format="+format); err != nil {
		return err
	}
	if _, err := g.runCommand("remote", "add", "origin", repoURL); err != nil {
		return err
	}
	authURL, err := g.formatAuthURL(repoURL)
	if err != nil {
		return err
	}
	if _, err := g.runCommand("fetch", "--depth=1", "--no-tags", "--", authURL, commitHash); err != nil {
		return err
	}
	if _, err := g.runCommand("checkout", "--detach", "FETCH_HEAD"); err != nil {
		return err
	}
	actual, err := g.runCommand("rev-parse", "--verify", "HEAD")
	if err != nil {
		return err
	}
	if actual != strings.ToLower(commitHash) {
		return fmt.Errorf("fetched commit does not match the selected object ID")
	}
	return nil
}

// Clone with authentication
func (g *GitShell) Clone(repoURL, branch string) error {
	authURL, err := g.formatAuthURL(repoURL)
	if err != nil {
		return err
	}

	args := []string{"clone"}
	if branch != "" {
		args = append(args, "-b", branch)
	}

	args = append(args, "--depth", "1")
	args = append(args, "--single-branch", authURL, g.workDir)

	_, err = g.runCommand(args...)
	return err
}
