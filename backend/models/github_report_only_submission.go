package models

import (
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"gorm.io/gorm"
)

// GithubReportOnlyOutcome is mutually exclusive with an execution graph.
// Its payloads retain the complete first-selected response to a delivery.
type GithubReportOnlyOutcome struct {
	Reason string `json:"reason"`
	Silent bool   `json:"silent,omitempty"`
}

func PrepareGithubSilentSubmission(reason string) (GithubSubmissionIntent, error) {
	return normalizeGithubReportOnlySubmission(GithubSubmissionIntent{ReportOnly: &GithubReportOnlyOutcome{Reason: reason, Silent: true}})
}

func PrepareGithubReportOnlySubmission(reason string, payloads []GithubReportCreatePayload) (GithubSubmissionIntent, error) {
	intent := GithubSubmissionIntent{ReportOnly: &GithubReportOnlyOutcome{Reason: reason}}
	for index, payload := range payloads {
		raw, err := PrepareGithubReportCreatePayload(payload)
		if err != nil {
			return GithubSubmissionIntent{}, err
		}
		intent.Reports = append(intent.Reports, GithubSubmissionReport{Key: "outcome:" + strconv.Itoa(index),
			Role: GithubSubmissionReportOutcome, Order: index, Payload: raw})
	}
	return normalizeGithubReportOnlySubmission(intent)
}

func normalizeGithubReportOnlySubmission(intent GithubSubmissionIntent) (GithubSubmissionIntent, error) {
	if intent.Graph != nil || intent.ReportOnly == nil || len(intent.Sources) != 0 ||
		intent.ReportOnly.Reason == "" || len(intent.ReportOnly.Reason) > 128 ||
		intent.ReportOnly.Reason != strings.TrimSpace(intent.ReportOnly.Reason) || !utf8.ValidString(intent.ReportOnly.Reason) {
		return intent, ErrGithubSubmissionIntent
	}
	if intent.ReportOnly.Silent != (len(intent.Reports) == 0) {
		return intent, ErrGithubSubmissionIntent
	}
	intent.Sources = []GithubSubmissionSource{}
	if intent.Reports == nil {
		intent.Reports = []GithubSubmissionReport{}
	}
	keys, orders := make(map[string]bool), make(map[int]bool)
	var first GithubReportCreatePayload
	var head string
	for index := range intent.Reports {
		report := &intent.Reports[index]
		if report.Role != GithubSubmissionReportOutcome || report.Optional || report.ProjectName != "" || report.SourceLocation != "" ||
			report.Key == "" || report.Key != strings.TrimSpace(report.Key) || !utf8.ValidString(report.Key) || keys[report.Key] || report.Order < 0 || orders[report.Order] {
			return intent, ErrGithubSubmissionIntent
		}
		keys[report.Key], orders[report.Order] = true, true
		payload, err := DecodeGithubReportCreatePayload(report.Payload)
		if err != nil {
			return intent, ErrGithubSubmissionIntent
		}
		if index == 0 {
			first = payload
		}
		if payload.OrganisationID != first.OrganisationID || payload.GithubAppID != first.GithubAppID ||
			payload.GithubInstallationID != first.GithubInstallationID || payload.RepoOwner != first.RepoOwner ||
			payload.RepoName != first.RepoName || payload.PullRequestNumber != first.PullRequestNumber {
			return intent, ErrGithubSubmissionIntent
		}
		if payload.ResourceKind == GithubReportResourceCheckRun {
			if head != "" && payload.HeadSHA != head {
				return intent, ErrGithubSubmissionIntent
			}
			head = payload.HeadSHA
		}
		report.Payload, err = CanonicalGithubReportCreatePayload(payload)
		if err != nil {
			return intent, err
		}
	}
	sort.Slice(intent.Reports, func(i, j int) bool { return intent.Reports[i].Key < intent.Reports[j].Key })
	return intent, nil
}

func validateGithubReportOnlySubmissionEnvelope(tx *gorm.DB, identity JobCreationIdentity, intent GithubSubmissionIntent, delivery *GithubWebhookDelivery, orgID uint) error {
	if _, err := normalizeGithubReportOnlySubmission(intent); err != nil {
		return err
	}
	target, err := loadGithubDeliveryTargetIntentTx(tx, identity, delivery, orgID)
	if err != nil {
		return err
	}
	for _, report := range intent.Reports {
		payload, err := DecodeGithubReportCreatePayload(report.Payload)
		if err != nil || payload.OrganisationID != orgID || payload.GithubAppID != delivery.GithubAppID ||
			delivery.InstallationID == nil || payload.GithubInstallationID != *delivery.InstallationID {
			return ErrGithubSubmissionTenant
		}
		if payload.RepoOwner != target.RepoOwner || payload.RepoName != target.RepoName || payload.PullRequestNumber != target.PullRequestNumber ||
			(payload.ResourceKind == GithubReportResourceCheckRun && payload.HeadSHA != target.HeadSHA) {
			return ErrGithubSubmissionConflict
		}
	}
	return nil
}
