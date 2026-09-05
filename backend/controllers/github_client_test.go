package controllers

import (
	"context"
	"net/http"
	"testing"
	"time"

	githubci "github.com/diggerhq/digger/libs/ci/github"
	"github.com/stretchr/testify/require"
)

func TestGithubServiceReadsEndWhenDeliveryContextIsCancelled(t *testing.T) {
	started := make(chan struct{})
	client, _ := reportTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := githubci.GithubService{Client: githubClientForContext(ctx, client), Owner: "owner", RepoName: "repo"}
	result := make(chan error, 1)
	go func() { _, err := service.GetApprovals(42); result <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("review request did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("review request survived delivery cancellation")
	}
}
