package backendapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/operation"
)

var errGithubOIDCUnavailable = errors.New("GitHub OIDC token service temporarily unavailable")

func BuildExecutionClaimRequest(repositoryFullName string, projectName string, operationID string, protocolVersion int, writerEpoch int64) (ExecutionClaimRequest, error) {
	runID, err := positiveEnvironmentInt64("GITHUB_RUN_ID")
	if err != nil {
		return ExecutionClaimRequest{}, err
	}
	runAttempt, err := positiveEnvironmentInt64("GITHUB_RUN_ATTEMPT")
	if err != nil {
		return ExecutionClaimRequest{}, err
	}
	executable, err := os.Executable()
	if err != nil {
		return ExecutionClaimRequest{}, fmt.Errorf("resolve Digger executable: %w", err)
	}
	file, err := os.Open(executable)
	if err != nil {
		return ExecutionClaimRequest{}, fmt.Errorf("open Digger executable: %w", err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return ExecutionClaimRequest{}, fmt.Errorf("hash Digger executable: %w", err)
	}
	actionRepository := strings.TrimSpace(os.Getenv("GITHUB_ACTION_REPOSITORY"))
	actionVersion := strings.TrimSpace(os.Getenv("GITHUB_ACTION_REF"))
	if actionRepository == "" || actionVersion == "" {
		return ExecutionClaimRequest{}, fmt.Errorf("GitHub action identity is unavailable")
	}
	workflowRef := strings.TrimSpace(os.Getenv("GITHUB_WORKFLOW_REF"))
	workflowSHA := strings.ToLower(strings.TrimSpace(os.Getenv("GITHUB_WORKFLOW_SHA")))
	if workflowRef == "" || workflowSHA == "" {
		return ExecutionClaimRequest{}, fmt.Errorf("GitHub workflow identity is unavailable")
	}
	return ExecutionClaimRequest{
		RepositoryFullName:  repositoryFullName,
		ProjectName:         projectName,
		OperationID:         operationID,
		RunID:               runID,
		RunAttempt:          runAttempt,
		WorkflowRef:         workflowRef,
		WorkflowSHA:         workflowSHA,
		ActionRef:           actionRepository + "@" + actionVersion,
		CLISHA256:           hex.EncodeToString(digest.Sum(nil)),
		ProtocolVersion:     protocolVersion,
		DispatchWriterEpoch: writerEpoch,
	}, nil
}

func positiveEnvironmentInt64(name string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(os.Getenv(name)), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func (d *DiggerApi) ClaimProjectJobExecution(repo string, projectName string, jobID string, request ExecutionClaimRequest) (*ExecutionClaimResponse, error) {
	return d.ClaimProjectJobExecutionContext(context.Background(), repo, projectName, jobID, request)
}

func (d *DiggerApi) ClaimProjectJobExecutionContext(ctx context.Context, repo string, projectName string, jobID string, request ExecutionClaimRequest) (*ExecutionClaimResponse, error) {
	if request.RepositoryFullName != repo || request.ProjectName != projectName || strings.TrimSpace(jobID) == "" {
		return nil, fmt.Errorf("execution claim route identity does not match its payload")
	}
	u, err := url.Parse(d.DiggerHost)
	if err != nil {
		return nil, fmt.Errorf("parse Digger backend URL: %w", err)
	}
	u.Path = filepath.Join(u.Path, "v1", "jobs", jobID, "execution-claims")
	client := d.HttpClient
	if client == nil {
		client = http.DefaultClient
	}
	claimClient := *client
	claimClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client = &claimClient
	deadline := time.Now().Add(2 * time.Minute)
	if request.ProtocolVersion >= operation.OIDCProtocolVersion {
		if request.ClaimExpiresAt.IsZero() {
			return nil, fmt.Errorf("durable execution claim has no deadline")
		}
		deadline = request.ClaimExpiresAt
	}
	ctx, cancelClaims := context.WithDeadline(ctx, deadline)
	defer cancelClaims()
	retryDelay := 200 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if request.ProtocolVersion >= operation.OIDCProtocolVersion {
			audience, err := operation.ExecutionClaimAudience(request.OperationID, jobID)
			if err != nil {
				return nil, fmt.Errorf("build execution claim audience: %w", err)
			}
			request.OIDCToken, err = d.githubOIDCToken(ctx, client, audience)
			if err != nil {
				if !errors.Is(err, errGithubOIDCUnavailable) {
					return nil, err
				}
				if err := waitExecutionClaimRetry(ctx, retryDelay); err != nil {
					return nil, err
				}
				retryDelay = min(2*retryDelay, 2*time.Second)
				continue
			}
		}
		payload, err := json.Marshal(request)
		if err != nil {
			return nil, fmt.Errorf("marshal execution claim: %w", err)
		}
		requestTimeout := time.Until(deadline)
		if requestTimeout > 15*time.Second {
			requestTimeout = 15 * time.Second
		}
		requestContext, cancelRequest := context.WithTimeout(ctx, requestTimeout)
		req, err := http.NewRequestWithContext(requestContext, http.MethodPost, u.String(), bytes.NewReader(payload))
		if err != nil {
			cancelRequest()
			return nil, fmt.Errorf("create execution claim request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", d.AuthToken))
		response, requestErr := client.Do(req)
		if requestErr == nil && response.StatusCode == http.StatusOK {
			receipt, decodeErr := decodeExecutionClaimResponse(response.Body)
			response.Body.Close()
			cancelRequest()
			if decodeErr == nil {
				if !receipt.Granted || receipt.ExecutionGrant == "" {
					return nil, fmt.Errorf("execution claim was not granted")
				}
				if receipt.GrantExpiresAt.IsZero() || !receipt.GrantExpiresAt.After(time.Now()) {
					return nil, fmt.Errorf("execution claim grant is already expired")
				}
				d.durableExecutionContext = &durableExecutionContext{
					RepositoryFullName:  repo,
					ProjectName:         projectName,
					DiggerJobID:         jobID,
					OperationID:         request.OperationID,
					ProtocolVersion:     request.ProtocolVersion,
					DispatchWriterEpoch: request.DispatchWriterEpoch,
					ExecutionGrant:      receipt.ExecutionGrant,
					GrantExpiresAt:      receipt.GrantExpiresAt,
				}
				return receipt, nil
			}
			// The grant may have committed before its response was interrupted.
			// Retry the same identity so the server can replay the persisted grant.
			requestErr = decodeErr
			response = nil
		}
		retryable := requestErr != nil
		responseStatus := 0
		if response != nil {
			responseStatus = response.StatusCode
			retryable = requestErr != nil || response.StatusCode == http.StatusTooEarly || response.StatusCode >= 500
			response.Body.Close()
		}
		cancelRequest()
		if !retryable {
			if requestErr != nil {
				return nil, fmt.Errorf("send execution claim: %w", requestErr)
			}
			return nil, fmt.Errorf("execution claim rejected with status %d", responseStatus)
		}
		if err := waitExecutionClaimRetry(ctx, retryDelay); err != nil {
			return nil, err
		}
		if retryDelay < 2*time.Second {
			retryDelay *= 2
			if retryDelay > 2*time.Second {
				retryDelay = 2 * time.Second
			}
		}
	}
}

func waitExecutionClaimRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func decodeExecutionClaimResponse(body io.Reader) (*ExecutionClaimResponse, error) {
	payload, err := io.ReadAll(io.LimitReader(body, 64*1024+1))
	if err != nil || len(payload) > 64*1024 {
		return nil, fmt.Errorf("execution claim response is incomplete or oversized")
	}
	var receipt ExecutionClaimResponse
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&receipt); err != nil {
		return nil, fmt.Errorf("execution claim response is incomplete or invalid")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("execution claim response contains trailing data")
	}
	return &receipt, nil
}

func (d *DiggerApi) githubOIDCToken(ctx context.Context, client *http.Client, audience string) (string, error) {
	if d.oidcTokenProvider != nil {
		return d.oidcTokenProvider(ctx, audience)
	}
	requestURL := strings.TrimSpace(os.Getenv("ACTIONS_ID_TOKEN_REQUEST_URL"))
	requestToken := strings.TrimSpace(os.Getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN"))
	if requestURL == "" || requestToken == "" {
		return "", fmt.Errorf("GitHub Actions OIDC token endpoint is unavailable")
	}
	u, err := url.Parse(requestURL)
	if err != nil {
		return "", fmt.Errorf("parse GitHub Actions OIDC token URL: %w", err)
	}
	query := u.Query()
	query.Set("audience", audience)
	u.RawQuery = query.Encode()
	requestContext, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestContext, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("create GitHub Actions OIDC token request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+requestToken)
	oidcClient := *client
	oidcClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := oidcClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: request failed", errGithubOIDCUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return "", errGithubOIDCUnavailable
		}
		return "", fmt.Errorf("GitHub Actions OIDC token endpoint returned status %d", response.StatusCode)
	}
	var body struct {
		Value string `json:"value"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64*1024))
	if err := decoder.Decode(&body); err != nil {
		return "", fmt.Errorf("%w: incomplete token response", errGithubOIDCUnavailable)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return "", fmt.Errorf("decode GitHub Actions OIDC token response: trailing JSON")
	}
	if strings.TrimSpace(body.Value) == "" || len(body.Value) > 32*1024 {
		return "", fmt.Errorf("GitHub Actions OIDC token response is invalid")
	}
	return body.Value, nil
}
