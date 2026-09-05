package controllers

import (
	"net/http"

	"github.com/google/go-github/v61/github"
)

// githubClientWithoutRedirects retains installation authentication without
// allowing a redirect to repeat a POST or forward credentials to another origin.
func githubClientWithoutRedirects(client *github.Client) *github.Client {
	httpClient := *client.Client()
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	isolated := github.NewClient(&httpClient)
	isolated.BaseURL, isolated.UploadURL, isolated.UserAgent = client.BaseURL, client.UploadURL, client.UserAgent
	return isolated
}
