package controllers

import (
	"context"
	"io"
	"net/http"
	"sync"

	"github.com/google/go-github/v61/github"
)

// githubClientForContext also bounds legacy service methods which create their
// own request contexts. Cancellation remains active through response-body reads.
func githubClientForContext(ctx context.Context, client *github.Client) *github.Client {
	isolated := githubClientWithoutRedirects(client)
	httpClient := *isolated.Client()
	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpClient.Transport = githubContextTransport{parent: ctx, transport: transport}
	bound := github.NewClient(&httpClient)
	bound.BaseURL, bound.UploadURL, bound.UserAgent = isolated.BaseURL, isolated.UploadURL, isolated.UserAgent
	return bound
}

type githubContextTransport struct {
	parent    context.Context
	transport http.RoundTripper
}

func (transport githubContextTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := transport.parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(request.Context())
	stop := context.AfterFunc(transport.parent, cancel)
	cleanup := func() { stop(); cancel() }
	response, err := transport.transport.RoundTrip(request.Clone(ctx))
	if err != nil {
		cleanup()
		return nil, err
	}
	if response.Body == nil {
		cleanup()
	} else {
		response.Body = &githubContextBody{ReadCloser: response.Body, cleanup: cleanup}
	}
	return response, nil
}

type githubContextBody struct {
	io.ReadCloser
	once    sync.Once
	cleanup func()
}

func (body *githubContextBody) Close() error {
	err := body.ReadCloser.Close()
	body.once.Do(body.cleanup)
	return err
}

// githubClientWithoutRedirects retains installation authentication without
// allowing a redirect to repeat a POST or forward credentials to another origin.
func githubClientWithoutRedirects(client *github.Client) *github.Client {
	httpClient := *client.Client()
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	isolated := github.NewClient(&httpClient)
	isolated.BaseURL, isolated.UploadURL, isolated.UserAgent = client.BaseURL, client.UploadURL, client.UserAgent
	return isolated
}
