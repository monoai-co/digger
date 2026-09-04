package ci_backends

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/diggerhq/digger/backend/utils"
	orchestrator_scheduler "github.com/diggerhq/digger/libs/scheduler"
	"github.com/diggerhq/digger/libs/spec"
	"github.com/google/go-github/v61/github"
)

type GithubActionCi struct {
	Client *github.Client
}

func (g GithubActionCi) TriggerWorkflow(spec spec.Spec, runName string, vcsToken string) error {
	return g.TriggerWorkflowContext(context.Background(), spec, runName, vcsToken)
}

func (g GithubActionCi) TriggerWorkflowContext(ctx context.Context, spec spec.Spec, runName string, vcsToken string) error {
	slog.Info("TriggerGithubWorkflow", "repoOwner", spec.VCS.RepoOwner, "repoName", spec.VCS.RepoName, "commentId", spec.CommentId)
	client := g.Client
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("marshal workflow specification: %w", err)
	}

	repository, _, err := client.Repositories.Get(ctx, spec.VCS.RepoOwner, spec.VCS.RepoName)
	if err != nil {
		return fmt.Errorf("resolve repository control ref: %w", err)
	}
	controlRef := repository.GetDefaultBranch()
	if controlRef == "" {
		return fmt.Errorf("resolve repository control ref: default branch is empty")
	}

	inputs := orchestrator_scheduler.WorkflowInput{
		Spec:    string(specBytes),
		RunName: runName,
	}

	_, err = client.Actions.CreateWorkflowDispatchEventByFileName(ctx, spec.VCS.RepoOwner, spec.VCS.RepoName, spec.VCS.WorkflowFile, github.CreateWorkflowDispatchEventRequest{
		Ref:    controlRef,
		Inputs: inputs.ToMap(),
	})

	return err
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
