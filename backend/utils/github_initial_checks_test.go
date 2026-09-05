package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	githubService "github.com/diggerhq/digger/libs/ci/github"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/go-github/v61/github"
	"github.com/stretchr/testify/require"
)

func TestRenderGithubInitialChecksGolden(t *testing.T) {
	tests := []struct {
		name string
		jobs []scheduler.JobJson
		want []GithubInitialCheck
	}{
		{"no jobs", nil, []GithubInitialCheck{
			{Role: GithubInitialCheckReportOnly, Check: models.GithubReportCheck{Name: "digger/plan", Status: "completed", Conclusion: "success", Title: "No impacted projects", Summary: "Check your configuration and files changed if this is unexpected", Text: "digger/plan"}},
			{Role: GithubInitialCheckReportOnly, Check: models.GithubReportCheck{Name: "digger/apply", Status: "completed", Conclusion: "success", Title: "No impacted projects", Summary: "Check your configuration and files changed if this is unexpected", Text: "digger/apply"}},
		}},
		{"aliased plan", []scheduler.JobJson{{ProjectName: "infra", ProjectAlias: "Infrastructure", Commands: []string{"digger plan"}}}, []GithubInitialCheck{
			{Role: GithubInitialCheckProject, ProjectName: "infra", Check: models.GithubReportCheck{Name: "Infrastructure/plan", Status: "in_progress", Title: "Waiting for plan...", Text: "Plan result will appear here"}},
			{Role: GithubInitialCheckBatch, Check: models.GithubReportCheck{Name: "digger/plan", Status: "in_progress", Title: "Pending start...", Text: "| Project | Status |\n|---------|--------|\n|:clock11: **Infrastructure**|pending...|\n"}},
			{Role: GithubInitialCheckCompanion, Optional: true, Check: models.GithubReportCheck{Name: "digger/apply", Status: "queued", Title: "Waiting for plan to complete...", Summary: "The apply check will automatically succeed if there are no changes to apply"}},
		}},
		{"apply", []scheduler.JobJson{{ProjectName: "infra", Commands: []string{"digger apply"}}}, []GithubInitialCheck{
			{Role: GithubInitialCheckProject, ProjectName: "infra", Check: models.GithubReportCheck{Name: "infra/apply", Status: "in_progress", Title: "Waiting for apply...", Text: "Apply result will appear here"}},
			{Role: GithubInitialCheckBatch, Check: models.GithubReportCheck{Name: "digger/apply", Status: "in_progress", Title: "Pending start...", Text: "| Project | Status |\n|---------|--------|\n|:clock11: **infra**|pending...|\n"}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before, err := json.Marshal(test.jobs)
			require.NoError(t, err)
			actual := RenderGithubInitialChecks(test.jobs)
			require.Equal(t, test.want, actual)
			after, err := json.Marshal(test.jobs)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestRenderGithubInitialChecksPreservesMixedRepeatedCommands(t *testing.T) {
	jobs := []scheduler.JobJson{
		{ProjectName: "b", Commands: []string{"digger apply", "custom command", "digger plan", "digger apply"}},
		{ProjectName: "a", Commands: []string{"digger plan", "digger plan"}},
	}
	checks := RenderGithubInitialChecks(jobs)
	names := make([]string, 0, len(checks))
	for _, check := range checks {
		names = append(names, check.Check.Name)
	}
	require.Equal(t, []string{"b/apply", "b/plan", "b/apply", "a/plan", "a/plan", "digger/plan", "digger/apply"}, names)
	require.True(t, checks[len(checks)-1].Optional)
	jobs = append(jobs, scheduler.JobJson{ProjectName: "c", Commands: []string{"custom command"}})
	checks = RenderGithubInitialChecks(jobs)
	require.Equal(t, GithubInitialCheckBatch, checks[len(checks)-1].Role)
	require.Equal(t, "digger/apply", checks[len(checks)-1].Check.Name)
	require.False(t, checks[len(checks)-1].Optional)
	checks[0].Check.Name = "changed"
	require.Equal(t, "b/apply", RenderGithubInitialChecks(jobs)[0].Check.Name)
}

func TestSetPRCheckForJobsHTTPParity(t *testing.T) {
	t.Setenv("DIGGER_GITHUB_HOSTNAME", "enterprise.example")
	for _, test := range []struct {
		name      string
		jobs      []scheduler.Job
		failAt    int
		wantError bool
		wantCalls int
	}{
		{"plan", []scheduler.Job{{ProjectName: "infra", ProjectAlias: "Alias", Commands: []string{"digger plan"}}}, 0, false, 3},
		{"apply", []scheduler.Job{{ProjectName: "infra", Commands: []string{"digger apply"}}}, 0, false, 2},
		{"empty", nil, 0, false, 2},
		{"optional companion fails", []scheduler.Job{{ProjectName: "infra", Commands: []string{"digger plan"}}}, 3, false, 3},
		{"project fails", []scheduler.Job{{ProjectName: "infra", Commands: []string{"digger plan"}}}, 1, true, 1},
		{"aggregate fails", []scheduler.Job{{ProjectName: "infra", Commands: []string{"digger plan"}}}, 2, true, 2},
		{"empty plan fails", nil, 1, true, 1},
		{"empty apply fails", nil, 2, true, 2},
		{"repeated commands", []scheduler.Job{{ProjectName: "infra", Commands: []string{"digger apply", "digger plan", "digger apply"}}}, 0, false, 5},
		{"mixed jobs", []scheduler.Job{{ProjectName: "one", Commands: []string{"digger plan"}}, {ProjectName: "two", Commands: []string{"digger apply"}}}, 0, false, 3},
		{"unrecognized commands", []scheduler.Job{{ProjectName: "infra", Commands: []string{"custom command"}}}, 0, false, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls []github.CreateCheckRunOptions
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/repos/owner/repo/check-runs" {
					http.Error(w, "unexpected route", 400)
					return
				}
				var options github.CreateCheckRunOptions
				if json.NewDecoder(r.Body).Decode(&options) != nil {
					http.Error(w, "invalid body", 400)
					return
				}
				calls = append(calls, options)
				w.Header().Set("Content-Type", "application/json")
				if test.failAt == len(calls) {
					w.WriteHeader(http.StatusUnprocessableEntity)
					w.Write([]byte(`{"message":"rejected"}`))
					return
				}
				fmt.Fprintf(w, `{"id":%d}`, len(calls))
			}))
			t.Cleanup(server.Close)
			client := github.NewClient(server.Client())
			base, err := url.Parse(server.URL + "/")
			require.NoError(t, err)
			client.BaseURL = base
			service := &githubService.GithubService{Client: client, Owner: "owner", RepoName: "repo"}
			batch, projects, err := SetPRCheckForJobs(service, 42, test.jobs, "commit", "repo", "owner")
			if test.wantError {
				require.Error(t, err)
				require.Nil(t, batch)
				require.Nil(t, projects)
			} else {
				require.NoError(t, err)
				require.NotNil(t, batch)
			}
			require.Len(t, calls, test.wantCalls)
			jobSpecs := make([]scheduler.JobJson, 0, len(test.jobs))
			for _, job := range test.jobs {
				jobSpecs = append(jobSpecs, scheduler.JobJson{ProjectName: job.ProjectName, ProjectAlias: job.ProjectAlias, Commands: job.Commands})
			}
			descriptors := RenderGithubInitialChecks(jobSpecs)
			lastProjectCall := make(map[string]int)
			batchCall := 0
			for index, call := range calls {
				descriptor := descriptors[index]
				check := descriptor.Check
				require.Equal(t, check.Name, call.Name)
				require.Equal(t, "commit", call.HeadSHA)
				require.Equal(t, check.Status, call.GetStatus())
				require.Equal(t, check.Conclusion, call.GetConclusion())
				require.Equal(t, check.Title, call.GetOutput().GetTitle())
				require.Equal(t, check.Summary, call.GetOutput().GetSummary())
				require.Equal(t, check.Text, call.GetOutput().GetText())
				require.Nil(t, call.ExternalID)
				require.Nil(t, call.StartedAt)
				require.Nil(t, call.CompletedAt)
				require.Empty(t, call.Actions)
				if descriptor.Role == GithubInitialCheckProject {
					lastProjectCall[descriptor.ProjectName] = index + 1
				}
				if descriptor.Role == GithubInitialCheckBatch {
					batchCall = index + 1
				}
			}
			if !test.wantError {
				if batchCall == 0 {
					require.Equal(t, &CheckRunData{}, batch)
				} else {
					require.Equal(t, fmt.Sprint(batchCall), batch.Id)
					require.Equal(t, fmt.Sprintf("https://enterprise.example/owner/repo/pull/42/checks?check_run_id=%d", batchCall), batch.Url)
				}
				require.Len(t, projects, len(lastProjectCall))
				for project, id := range lastProjectCall {
					require.Equal(t, fmt.Sprint(id), projects[project].Id)
				}
			}
		})
	}
}
