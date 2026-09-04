package backendapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

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
	if request.RepositoryFullName != repo || request.ProjectName != projectName || strings.TrimSpace(jobID) == "" {
		return nil, fmt.Errorf("execution claim route identity does not match its payload")
	}
	u, err := url.Parse(d.DiggerHost)
	if err != nil {
		return nil, fmt.Errorf("parse Digger backend URL: %w", err)
	}
	u.Path = filepath.Join(u.Path, "v1", "jobs", jobID, "execution-claims")
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal execution claim: %w", err)
	}
	client := d.HttpClient
	if client == nil {
		client = http.DefaultClient
	}
	deadline := time.Now().Add(2 * time.Minute)
	retryDelay := 200 * time.Millisecond
	for {
		requestTimeout := time.Until(deadline)
		if requestTimeout > 15*time.Second {
			requestTimeout = 15 * time.Second
		}
		requestContext, cancelRequest := context.WithTimeout(context.Background(), requestTimeout)
		req, err := http.NewRequestWithContext(requestContext, http.MethodPost, u.String(), bytes.NewReader(payload))
		if err != nil {
			cancelRequest()
			return nil, fmt.Errorf("create execution claim request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", d.AuthToken))
		response, requestErr := client.Do(req)
		if requestErr == nil && response.StatusCode == http.StatusOK {
			var receipt ExecutionClaimResponse
			decoder := json.NewDecoder(response.Body)
			if err := decoder.Decode(&receipt); err != nil {
				response.Body.Close()
				cancelRequest()
				return nil, fmt.Errorf("decode execution claim response: %w", err)
			}
			response.Body.Close()
			cancelRequest()
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
			return &receipt, nil
		}
		retryable := requestErr != nil
		responseStatus := 0
		if response != nil {
			responseStatus = response.StatusCode
			retryable = response.StatusCode == http.StatusTooEarly || response.StatusCode == http.StatusServiceUnavailable
			response.Body.Close()
		}
		cancelRequest()
		if !retryable || time.Now().Add(retryDelay).After(deadline) {
			if requestErr != nil {
				return nil, fmt.Errorf("send execution claim: %w", requestErr)
			}
			return nil, fmt.Errorf("execution claim rejected with status %d", responseStatus)
		}
		time.Sleep(retryDelay)
		if retryDelay < 2*time.Second {
			retryDelay *= 2
			if retryDelay > 2*time.Second {
				retryDelay = 2 * time.Second
			}
		}
	}
}
