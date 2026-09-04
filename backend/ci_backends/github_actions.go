package ci_backends

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/diggerhq/digger/backend/utils"
	orchestrator_scheduler "github.com/diggerhq/digger/libs/scheduler"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/google/go-github/v61/github"
)

type GithubActionCi struct {
	Client *github.Client
}

var ErrWorkflowDispatchAcceptanceAmbiguous = errors.New("GitHub workflow dispatch acceptance is ambiguous")

type WorkflowDispatchRunDetails struct {
	RunID   int64  `json:"workflow_run_id"`
	RunURL  string `json:"run_url"`
	HTMLURL string `json:"html_url"`
}

func (g GithubActionCi) TriggerWorkflow(spec spec.Spec, runName string, vcsToken string) error {
	return g.TriggerWorkflowContext(context.Background(), spec, runName, vcsToken)
}

func (g GithubActionCi) TriggerWorkflowContext(ctx context.Context, spec spec.Spec, runName string, vcsToken string) error {
	slog.Info("TriggerGithubWorkflow", "repoOwner", spec.VCS.RepoOwner, "repoName", spec.VCS.RepoName, "commentId", spec.CommentId)
	controlRef, err := g.ResolveControlRef(ctx, spec.VCS.RepoOwner, spec.VCS.RepoName)
	if err != nil {
		return err
	}
	return g.TriggerWorkflowContextAtRef(ctx, spec, runName, vcsToken, controlRef)
}

func (g GithubActionCi) ResolveControlRef(ctx context.Context, owner string, repo string) (string, error) {
	repository, _, err := g.Client.Repositories.Get(ctx, owner, repo)
	if err != nil {
		return "", fmt.Errorf("resolve repository control ref: %w", err)
	}
	controlRef := repository.GetDefaultBranch()
	if controlRef == "" {
		return "", fmt.Errorf("resolve repository control ref: default branch is empty")
	}
	return controlRef, nil
}

func (g GithubActionCi) TriggerWorkflowContextAtRef(ctx context.Context, spec spec.Spec, runName string, vcsToken string, controlRef string) error {
	if controlRef == "" {
		return fmt.Errorf("dispatch repository control ref: ref is empty")
	}
	inputs, err := workflowDispatchInputs(spec, runName)
	if err != nil {
		return err
	}
	_, err = g.Client.Actions.CreateWorkflowDispatchEventByFileName(ctx, spec.VCS.RepoOwner, spec.VCS.RepoName, spec.VCS.WorkflowFile, github.CreateWorkflowDispatchEventRequest{
		Ref:    controlRef,
		Inputs: inputs,
	})
	return err
}

func (g GithubActionCi) TriggerWorkflowContextAtRefWithRunDetails(ctx context.Context, spec spec.Spec, runName string, vcsToken string, controlRef string) (*WorkflowDispatchRunDetails, error) {
	if controlRef == "" {
		return nil, fmt.Errorf("dispatch repository control ref: ref is empty")
	}
	inputs, err := workflowDispatchInputs(spec, runName)
	if err != nil {
		return nil, err
	}

	body := struct {
		Ref              string                 `json:"ref"`
		Inputs           map[string]interface{} `json:"inputs,omitempty"`
		ReturnRunDetails bool                   `json:"return_run_details"`
	}{
		Ref:              controlRef,
		Inputs:           inputs,
		ReturnRunDetails: true,
	}
	requestPath := fmt.Sprintf("repos/%v/%v/actions/workflows/%v/dispatches", spec.VCS.RepoOwner, spec.VCS.RepoName, spec.VCS.WorkflowFile)
	request, err := g.Client.NewRequest(http.MethodPost, requestPath, body)
	if err != nil {
		return nil, fmt.Errorf("create GitHub workflow dispatch request: %w", err)
	}
	request.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	details := new(WorkflowDispatchRunDetails)
	response, err := g.Client.Do(ctx, request, details)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrWorkflowDispatchAcceptanceAmbiguous, err)
	}
	if response == nil || response.StatusCode != http.StatusOK || details.RunID <= 0 || details.RunURL == "" || details.HTMLURL == "" {
		statusCode := 0
		if response != nil {
			statusCode = response.StatusCode
		}
		return nil, fmt.Errorf("%w: response status %d did not include run details", ErrWorkflowDispatchAcceptanceAmbiguous, statusCode)
	}
	return details, nil
}

func workflowDispatchInputs(workflowSpec spec.Spec, runName string) (map[string]interface{}, error) {
	specBytes, err := json.Marshal(workflowSpec)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow specification: %w", err)
	}
	return (&orchestrator_scheduler.WorkflowInput{Spec: string(specBytes), RunName: runName}).ToMap(), nil
}

func (g GithubActionCi) GetWorkflowUrl(spec spec.Spec) (string, error) {
	if spec.JobId == "" {
		slog.Error("Cannot get workflow URL: JobId is empty")
		return "", fmt.Errorf("job ID is required to fetch workflow URL")
	}

	_, workflowRunUrl, err := utils.GetWorkflowIdAndUrlFromDiggerJobId(g.Client, spec.VCS.RepoOwner, spec.VCS.RepoName, spec.JobId)
	if err != nil {
		return "", err
	} else {
		return workflowRunUrl, nil
	}
}
