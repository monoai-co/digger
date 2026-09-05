//go:build unix

package git_utils

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCloneComparisonUsesSelectedCommitsAndCompleteFileList(t *testing.T) {
	if err := exec.Command("git", "var", "GIT_AUTHOR_IDENT").Run(); err != nil {
		t.Skip("git fixture commits require a configured author")
	}
	source := t.TempDir()
	gitFixtureCommand(t, source, "init", "-b", "main")
	write := func(name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(source, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(message string) string {
		t.Helper()
		gitFixtureCommand(t, source, "add", "--all")
		gitFixtureCommand(t, source, "commit", "-m", message)
		return gitFixtureCommand(t, source, "rev-parse", "HEAD")
	}
	write("old name.tf")
	commit("Base fixture")
	gitFixtureCommand(t, source, "checkout", "-b", "feature")
	gitFixtureCommand(t, source, "mv", "old name.tf", "new name.tf")
	for index := 0; index < 400; index++ {
		write(fmt.Sprintf("project-%03d.tf", index))
	}
	write(" newline\nfile.tf ")
	head := commit("Selected fixture head")
	write("later-head.tf")
	commit("Later fixture head")
	gitFixtureCommand(t, source, "checkout", "main")
	write("base-only.tf")
	base := commit("Advanced fixture base")
	remote := (&url.URL{Scheme: "file", Path: source}).String()
	clones := t.TempDir()
	t.Setenv("TMPDIR", clones)
	var files []string
	err := CloneComparisonAndDoAction(context.Background(), remote, base, head, "", "", func(directory string, changed []string) error {
		if actual := gitFixtureCommand(t, directory, "rev-parse", "HEAD"); actual != head {
			t.Fatalf("wrong checkout: %s", actual)
		}
		if _, err := os.Stat(filepath.Join(directory, "later-head.tf")); !os.IsNotExist(err) {
			t.Fatalf("later head content was included: %v", err)
		}
		files = changed
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 403 || !slices.Contains(files, "old name.tf") || !slices.Contains(files, "new name.tf") || !slices.Contains(files, " newline\nfile.tf ") ||
		slices.Contains(files, "base-only.tf") || slices.Contains(files, "later-head.tf") {
		t.Fatalf("incorrect fixed comparison: %v", files)
	}
	entries, err := os.ReadDir(clones)
	if err != nil || len(entries) != 0 {
		t.Fatalf("comparison clone was not removed: %v, %v", entries, err)
	}
}

func TestCloneComparisonCancellationStopsFetch(t *testing.T) {
	requested := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested <- struct{}{}
		<-r.Context().Done()
		closed <- struct{}{}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	clones := t.TempDir()
	t.Setenv("TMPDIR", clones)
	go func() {
		finished <- CloneComparisonAndDoAction(ctx, server.URL, strings.Repeat("a", 40), strings.Repeat("b", 40), "", "", func(string, []string) error {
			return fmt.Errorf("unexpected callback")
		})
	}()
	select {
	case <-requested:
	case err := <-finished:
		t.Fatalf("fetch exited before request: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("fetch did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("cancelled fetch succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled fetch did not exit")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled git process retained its HTTP connection")
	}
	entries, err := os.ReadDir(clones)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cancelled comparison left a clone: %v, %v", entries, err)
	}
}
