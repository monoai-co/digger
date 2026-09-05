package git_utils

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// CloneComparisonAndDoAction reads the head tree and complete three-dot file
// comparison from immutable commits. Neither branch names nor paginated provider
// file lists participate in the selection.
func CloneComparisonAndDoAction(ctx context.Context, repoURL, baseSHA, headSHA, token, tokenUsername string, action func(string, []string) error) error {
	base, baseErr := hex.DecodeString(baseSHA)
	head, headErr := hex.DecodeString(headSHA)
	if baseErr != nil || headErr != nil || len(base) != len(head) || (len(base) != 20 && len(base) != 32) {
		return fmt.Errorf("comparison requires full commit object IDs in the same format")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	directory, err := createTempDir()
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(directory); err != nil {
			slog.Warn("Failed to remove comparison directory", "directory", directory, "error", err)
		}
	}()
	git := NewGitShellWithTokenAuth(directory, token, tokenUsername)
	git.parentContext = ctx
	format := "sha1"
	if len(base) == 32 {
		format = "sha256"
	}
	if _, err := git.runCommand("init", "--object-format="+format); err != nil {
		return err
	}
	if _, err := git.runCommand("remote", "add", "origin", repoURL); err != nil {
		return err
	}
	authURL, err := git.formatAuthURL(repoURL)
	if err != nil {
		return err
	}
	// Both ancestry chains are required to identify their merge base. A shallow
	// fetch could omit it and silently change which projects are impacted.
	if _, err := git.runCommand("fetch", "--no-tags", "--", authURL, baseSHA, headSHA); err != nil {
		return err
	}
	if _, err := git.runCommand("checkout", "--detach", headSHA); err != nil {
		return err
	}
	actual, err := git.runCommand("rev-parse", "--verify", "HEAD")
	if err != nil {
		return err
	}
	if actual != strings.ToLower(headSHA) {
		return fmt.Errorf("comparison checkout does not match selected head")
	}
	mergeBase, err := git.runCommand("merge-base", "--all", baseSHA, headSHA)
	if err != nil {
		return fmt.Errorf("resolve comparison merge base: %w", err)
	}
	decodedBase, err := hex.DecodeString(mergeBase)
	if err != nil || len(decodedBase) != len(base) {
		return fmt.Errorf("comparison does not have one unambiguous merge base")
	}
	raw, err := git.runCommandRaw("diff", "--name-only", "--no-renames", "-z", mergeBase, headSHA, "--")
	if err != nil {
		return err
	}
	files := []string{}
	if raw != "" {
		if !strings.HasSuffix(raw, "\x00") {
			return fmt.Errorf("comparison file list is not NUL terminated")
		}
		files = strings.Split(strings.TrimSuffix(raw, "\x00"), "\x00")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return action(directory, files)
}
