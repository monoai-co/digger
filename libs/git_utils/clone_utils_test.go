package git_utils

import (
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func gitFixtureCommand(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git fixture command %v: %v: %s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestCloneSelectedCommitAfterBranchAdvances(t *testing.T) {
	// Use the existing configured author, never override Git identity in tests.
	if err := exec.Command("git", "var", "GIT_AUTHOR_IDENT").Run(); err != nil {
		t.Skip("git fixture commits require a configured author")
	}
	source := t.TempDir()
	gitFixtureCommand(t, source, "init", "-b", "main")
	gitFixtureCommand(t, source, "commit", "--allow-empty", "-m", "First fixture commit")
	selected := gitFixtureCommand(t, source, "rev-parse", "HEAD")
	gitFixtureCommand(t, source, "commit", "--allow-empty", "-m", "Second fixture commit")
	remote := (&url.URL{Scheme: "file", Path: source}).String()
	tip := gitFixtureCommand(t, source, "rev-parse", "HEAD")
	for _, test := range []struct {
		name, branch, commit, expected string
		wantError                      bool
	}{
		{name: "advanced branch", branch: "main", commit: selected, expected: selected},
		{name: "absent branch", branch: "deleted", commit: selected, expected: selected},
		{name: "branch only unchanged", branch: "main", expected: tip},
		{name: "missing object", branch: "main", commit: strings.Repeat("f", 40), wantError: true},
		{name: "non-object ref", branch: "main", commit: "HEAD", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			clones := t.TempDir()
			t.Setenv("TMPDIR", clones)
			var checkedOut string
			err := CloneGitRepoAndDoAction(remote, test.branch, test.commit, "", "", func(directory string) error {
				checkedOut = gitFixtureCommand(t, directory, "rev-parse", "HEAD")
				if test.commit != "" && gitFixtureCommand(t, directory, "remote", "get-url", "origin") != remote {
					t.Error("pinned clone did not retain the repository origin")
				}
				return nil
			})
			if (err != nil) != test.wantError {
				t.Fatalf("clone error = %v, want error %v", err, test.wantError)
			}
			if checkedOut != test.expected {
				t.Fatalf("checked out %s, want %s", checkedOut, test.expected)
			}
			entries, err := os.ReadDir(clones)
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary clone was not cleaned up: %v, %v", entries, err)
			}
		})
	}
}
