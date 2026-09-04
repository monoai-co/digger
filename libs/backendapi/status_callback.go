package backendapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
)

const durableStatusExpectedVersion int64 = 2

const defaultDurableStatusRetryDelay = 200 * time.Millisecond
const maximumDurableStatusRetryDelay = 2 * time.Second
const durableStatusRequestTimeout = 15 * time.Second

type durableExecutionContext struct {
	RepositoryFullName  string
	ProjectName         string
	DiggerJobID         string
	OperationID         string
	ProtocolVersion     int
	DispatchWriterEpoch int64
	ExecutionGrant      string
	GrantExpiresAt      time.Time
}

type durableStatusCallbackRequest struct {
	CallbackID            uuid.UUID `json:"callback_id"`
	RepositoryFullName    string    `json:"repository_full_name"`
	ProjectName           string    `json:"project_name"`
	OperationID           string    `json:"operation_id"`
	ProtocolVersion       int       `json:"protocol_version"`
	DispatchWriterEpoch   int64     `json:"dispatch_writer_epoch"`
	TargetStatus          string    `json:"target_status"`
	ExpectedStatusVersion int64     `json:"expected_status_version"`
	ClientTimestamp       time.Time `json:"client_timestamp"`
	JobSummary            any       `json:"job_summary"`
	JobPlanFootprint      any       `json:"job_plan_footprint"`
	PrCommentURL          string    `json:"pr_comment_url"`
	PrCommentID           string    `json:"pr_comment_id"`
	TerraformOutput       string    `json:"terraform_output"`
	WorkflowURL           string    `json:"workflow_url"`
}

func (d DiggerApi) hasDurableExecutionContext(repo string, projectName string, jobID string) bool {
	context := d.durableExecutionContext
	return context != nil && context.RepositoryFullName == repo && context.ProjectName == projectName && context.DiggerJobID == jobID
}

func (d DiggerApi) reportDurableProjectJobStatus(
	repo string,
	projectName string,
	jobID string,
	status string,
	timestamp time.Time,
	jobSummary any,
	jobPlanFootprint any,
	prCommentURL string,
	prCommentID string,
	terraformOutput string,
	workflowURL string,
) (*scheduler.SerializedBatch, error) {
	executionContext := d.durableExecutionContext
	if executionContext == nil {
		return nil, fmt.Errorf("durable execution context is unavailable")
	}
	callbackID := uuid.New()
	request := durableStatusCallbackRequest{
		CallbackID:            callbackID,
		RepositoryFullName:    repo,
		ProjectName:           projectName,
		OperationID:           executionContext.OperationID,
		ProtocolVersion:       executionContext.ProtocolVersion,
		DispatchWriterEpoch:   executionContext.DispatchWriterEpoch,
		TargetStatus:          status,
		ExpectedStatusVersion: durableStatusExpectedVersion,
		ClientTimestamp:       timestamp,
		JobSummary:            jobSummary,
		JobPlanFootprint:      jobPlanFootprint,
		PrCommentURL:          prCommentURL,
		PrCommentID:           prCommentID,
		TerraformOutput:       terraformOutput,
		WorkflowURL:           workflowURL,
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal durable status callback: %w", err)
	}
	u, err := url.Parse(d.DiggerHost)
	if err != nil {
		return nil, fmt.Errorf("parse Digger backend URL: %w", err)
	}
	u.Path = path.Join(u.Path, "v1", "jobs", jobID, "status-callbacks", callbackID.String())

	client := durableStatusHTTPClient(d.HttpClient)
	deadline := executionContext.GrantExpiresAt
	if deadline.IsZero() {
		return nil, fmt.Errorf("durable status callback grant expiry is unavailable")
	}
	if d.durableStatusRetryWindow > 0 {
		testDeadline := time.Now().Add(d.durableStatusRetryWindow)
		if testDeadline.Before(deadline) {
			deadline = testDeadline
		}
	}
	retryDelay := d.durableStatusRetryDelay
	if retryDelay <= 0 {
		retryDelay = defaultDurableStatusRetryDelay
	}
	for {
		requestTimeout := time.Until(deadline)
		if requestTimeout <= 0 {
			return nil, fmt.Errorf("durable status callback retry deadline exceeded")
		}
		if requestTimeout > durableStatusRequestTimeout {
			requestTimeout = durableStatusRequestTimeout
		}
		requestContext, cancelRequest := context.WithTimeout(context.Background(), requestTimeout)
		httpRequest, requestErr := http.NewRequestWithContext(requestContext, http.MethodPost, u.String(), bytes.NewReader(payload))
		if requestErr != nil {
			cancelRequest()
			return nil, fmt.Errorf("create durable status callback request: %w", requestErr)
		}
		httpRequest.Header.Set("Content-Type", "application/json")
		httpRequest.Header.Set("Authorization", fmt.Sprintf("Bearer %s", d.AuthToken))
		httpRequest.Header.Set("X-Digger-Execution-Grant", executionContext.ExecutionGrant)

		response, requestErr := client.Do(httpRequest)
		var responseErr error
		if requestErr == nil && response.StatusCode == http.StatusOK {
			batch, decodeErr := decodeDurableStatusCallbackResponse(response.Body)
			closeErr := response.Body.Close()
			cancelRequest()
			if decodeErr == nil && closeErr == nil {
				return batch, nil
			}
			if decodeErr != nil {
				responseErr = decodeErr
			} else {
				responseErr = fmt.Errorf("close durable status callback response: %w", closeErr)
			}
		}

		retryable := requestErr != nil || responseErr != nil
		responseStatus := 0
		if response != nil && responseErr == nil {
			responseStatus = response.StatusCode
			retryable = response.StatusCode == http.StatusTooEarly || response.StatusCode == http.StatusServiceUnavailable || response.StatusCode >= http.StatusInternalServerError
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		cancelRequest()
		if !retryable || time.Now().Add(retryDelay).After(deadline) {
			if responseErr != nil {
				return nil, responseErr
			}
			if requestErr != nil {
				return nil, fmt.Errorf("send durable status callback: %w", requestErr)
			}
			return nil, fmt.Errorf("durable status callback rejected with status %d", responseStatus)
		}
		time.Sleep(retryDelay)
		if retryDelay < maximumDurableStatusRetryDelay {
			retryDelay *= 2
			if retryDelay > maximumDurableStatusRetryDelay {
				retryDelay = maximumDurableStatusRetryDelay
			}
		}
	}
}

func durableStatusHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	callbackClient := *client
	callbackClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &callbackClient
}

func decodeDurableStatusCallbackResponse(body io.Reader) (*scheduler.SerializedBatch, error) {
	decoder := json.NewDecoder(body)
	var response scheduler.SerializedBatch
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("decode durable status callback response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode durable status callback response: trailing JSON content")
	}
	return &response, nil
}
