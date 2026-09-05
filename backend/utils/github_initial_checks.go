package utils

import (
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
)

type GithubInitialCheckRole string

const (
	GithubInitialCheckProject    GithubInitialCheckRole = "project"
	GithubInitialCheckBatch      GithubInitialCheckRole = "batch"
	GithubInitialCheckCompanion  GithubInitialCheckRole = "companion"
	GithubInitialCheckReportOnly GithubInitialCheckRole = "report_only"
)

// GithubInitialCheck describes one ordered create request and its legacy
// binding. Provider IDs are deliberately absent from rendering inputs/outputs.
type GithubInitialCheck struct {
	Check       models.GithubReportCheck
	Role        GithubInitialCheckRole
	ProjectName string
	Optional    bool
}

// RenderGithubInitialChecks accepts frozen scheduler specs without constructing
// runtime jobs or provider clients. Commands retain their original ordering.
func RenderGithubInitialChecks(jobs []scheduler.JobJson) []GithubInitialCheck {
	checks := make([]GithubInitialCheck, 0)
	allPlan := true
	for _, job := range jobs {
		allPlan = allPlan && job.IsPlan()
		for _, command := range job.Commands {
			var check models.GithubReportCheck
			switch command {
			case "digger plan":
				check = models.GithubReportCheck{Name: job.GetProjectAlias() + "/plan", Status: "in_progress", Title: "Waiting for plan...", Text: "Plan result will appear here"}
			case "digger apply":
				check = models.GithubReportCheck{Name: job.GetProjectAlias() + "/apply", Status: "in_progress", Title: "Waiting for apply...", Text: "Apply result will appear here"}
			default:
				continue
			}
			checks = append(checks, GithubInitialCheck{Check: check, Role: GithubInitialCheckProject, ProjectName: job.ProjectName})
		}
	}
	if len(jobs) == 0 {
		for _, command := range []string{"plan", "apply"} {
			checks = append(checks, GithubInitialCheck{Role: GithubInitialCheckReportOnly,
				Check: models.GithubReportCheck{Name: "digger/" + command, Status: "completed", Conclusion: "success", Title: "No impacted projects", Summary: "Check your configuration and files changed if this is unexpected", Text: "digger/" + command}})
		}
		return checks
	}
	aggregate := "digger/apply"
	if allPlan {
		aggregate = "digger/plan"
	}
	checks = append(checks, GithubInitialCheck{Role: GithubInitialCheckBatch,
		Check: models.GithubReportCheck{Name: aggregate, Status: "in_progress", Title: "Pending start...", Text: GetInitialJobSummaryFromJobSpecs(jobs)}})
	if allPlan {
		checks = append(checks, GithubInitialCheck{Role: GithubInitialCheckCompanion, Optional: true,
			Check: models.GithubReportCheck{Name: "digger/apply", Status: "queued", Title: "Waiting for plan to complete...", Summary: "The apply check will automatically succeed if there are no changes to apply"}})
	}
	return checks
}
