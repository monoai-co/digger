package models

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const GithubReportCreateEffectKind = "github_report_create"

type GithubReportResourceKind string

const (
	GithubReportResourceComment  GithubReportResourceKind = "comment"
	GithubReportResourceCheckRun GithubReportResourceKind = "check_run"
)

// GithubReportCreateMaxBytes is the local contract bound, not a provider limit.
const GithubReportCreateMaxBytes = 1 << 20

var ErrGithubReportCreatePayload = errors.New("github report create payload is invalid")
var ErrGithubReportCreateCorrelation = errors.New("github report create correlation identity is invalid")

type GithubReportCreatePayload struct {
	OrganisationID       uint                     `json:"organisation_id"`
	GithubAppID          int64                    `json:"github_app_id"`
	GithubInstallationID int64                    `json:"github_installation_id"`
	RepoOwner            string                   `json:"repo_owner"`
	RepoName             string                   `json:"repo_name"`
	PullRequestNumber    int                      `json:"pull_request_number"`
	HeadSHA              string                   `json:"head_sha"`
	ResourceKind         GithubReportResourceKind `json:"resource_kind"`
	Body                 string                   `json:"body"`
	Check                *GithubReportCheck       `json:"check"`
}

type GithubReportCheck struct {
	Name       string                    `json:"name"`
	Status     string                    `json:"status"`
	Conclusion string                    `json:"conclusion"`
	Title      string                    `json:"title"`
	Summary    string                    `json:"summary"`
	Text       string                    `json:"text"`
	Actions    []GithubReportCheckAction `json:"actions"`
}

type GithubReportCheckAction struct {
	Label       string `json:"label"`
	Description string `json:"description"`
	Identifier  string `json:"identifier"`
}

// GithubReportCreateReceipt binds a provider resource to its immutable intent.
// It is a value contract, not an independently persisted model.
type GithubReportCreateReceipt struct {
	EffectID      uuid.UUID                `json:"effect_id"`
	PayloadSHA256 string                   `json:"payload_sha256"`
	ResourceKind  GithubReportResourceKind `json:"resource_kind"`
	ProviderID    int64                    `json:"provider_id"`
	ProviderURL   string                   `json:"provider_url"`
}

func DecodeGithubReportCreatePayload(raw []byte) (GithubReportCreatePayload, error) {
	var payload GithubReportCreatePayload
	if len(raw) > GithubReportCreateMaxBytes || !utf8.Valid(raw) {
		return payload, ErrGithubReportCreatePayload
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return GithubReportCreatePayload{}, ErrGithubReportCreatePayload
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return GithubReportCreatePayload{}, ErrGithubReportCreatePayload
	}
	if err := validateGithubReportCreatePayload(payload); err != nil {
		return GithubReportCreatePayload{}, err
	}
	if payload.Check != nil && payload.Check.Actions == nil {
		payload.Check.Actions = []GithubReportCheckAction{}
	}
	return payload, nil
}

// CanonicalGithubReportCreatePayload preserves report content verbatim while
// making optional collection representation stable across PostgreSQL JSONB.
func CanonicalGithubReportCreatePayload(payload GithubReportCreatePayload) ([]byte, error) {
	if err := validateGithubReportCreatePayload(payload); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > GithubReportCreateMaxBytes {
		return nil, ErrGithubReportCreatePayload
	}
	normalized, err := DecodeGithubReportCreatePayload(raw)
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(normalized)
	if err != nil || len(canonical) > GithubReportCreateMaxBytes {
		return nil, ErrGithubReportCreatePayload
	}
	return canonical, nil
}

func validateGithubReportCreatePayload(payload GithubReportCreatePayload) error {
	if payload.OrganisationID == 0 || payload.GithubAppID <= 0 || payload.GithubInstallationID <= 0 ||
		payload.PullRequestNumber <= 0 || !validGithubReportPathSegment(payload.RepoOwner) || !validGithubReportPathSegment(payload.RepoName) ||
		!utf8.ValidString(payload.RepoOwner) || !utf8.ValidString(payload.RepoName) || !utf8.ValidString(payload.HeadSHA) || !utf8.ValidString(payload.Body) {
		return ErrGithubReportCreatePayload
	}
	switch payload.ResourceKind {
	case GithubReportResourceComment:
		if payload.Check != nil || strings.TrimSpace(payload.Body) == "" || payload.HeadSHA != "" {
			return ErrGithubReportCreatePayload
		}
	case GithubReportResourceCheckRun:
		if payload.Check == nil || payload.Body != "" || !validGithubReportPathSegment(payload.HeadSHA) {
			return ErrGithubReportCreatePayload
		}
		check := payload.Check
		if strings.TrimSpace(check.Name) == "" || strings.TrimSpace(check.Title) == "" || len(check.Actions) > 3 ||
			!utf8.ValidString(check.Name) || !utf8.ValidString(check.Title) || !utf8.ValidString(check.Summary) || !utf8.ValidString(check.Text) {
			return ErrGithubReportCreatePayload
		}
		switch check.Status {
		case "queued", "in_progress":
			if check.Conclusion != "" {
				return ErrGithubReportCreatePayload
			}
		case "completed":
			switch check.Conclusion {
			case "action_required", "cancelled", "failure", "neutral", "success", "skipped", "timed_out":
			default:
				return ErrGithubReportCreatePayload
			}
		default:
			return ErrGithubReportCreatePayload
		}
		for _, action := range check.Actions {
			if strings.TrimSpace(action.Label) == "" || strings.TrimSpace(action.Description) == "" || strings.TrimSpace(action.Identifier) == "" ||
				utf8.RuneCountInString(action.Label) > 20 || utf8.RuneCountInString(action.Description) > 40 || utf8.RuneCountInString(action.Identifier) > 20 ||
				!utf8.ValidString(action.Label) || !utf8.ValidString(action.Description) || !utf8.ValidString(action.Identifier) {
				return ErrGithubReportCreatePayload
			}
		}
	default:
		return ErrGithubReportCreatePayload
	}
	return nil
}

// Provider clients interpolate these values into URL paths without escaping.
func validGithubReportPathSegment(value string) bool {
	if value == "" || value != strings.TrimSpace(value) || value == "." || value == ".." || strings.ContainsAny(value, "/\\?#%") {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return utf8.ValidString(value)
}

// GithubReportCreateCorrelation derives provider correlation exclusively from
// the persisted outbox identity. It cannot be supplied as report payload data.
func GithubReportCreateCorrelation(effectID uuid.UUID, digest string) (string, error) {
	decoded, err := hex.DecodeString(digest)
	if effectID == uuid.Nil || err != nil || len(decoded) != 32 || strings.ToLower(digest) != digest {
		return "", ErrGithubReportCreateCorrelation
	}
	return "digger-report:" + effectID.String() + ":" + digest, nil
}
