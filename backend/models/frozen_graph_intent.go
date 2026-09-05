package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/dominikbraun/graph"
)

var ErrFrozenGraphIntent = errors.New("frozen execution graph intent is invalid")

// NormalizeDurableGraphIntent is the shared persistence and execution boundary.
// Its returned jobs are canonical by name; order preserves stable DAG traversal.
func NormalizeDurableGraphIntent(deliveryOperationID string, intent DurableGraphIntent) (*DurableGraphIntent, []string, error) {
	normalized, order, err := normalizeFrozenGraphShape(intent)
	if err != nil {
		return nil, nil, err
	}
	deliveryID := operation.ID(deliveryOperationID)
	if !deliveryID.Valid() {
		return nil, nil, ErrFrozenGraphIntent
	}
	batchID, err := operation.DeriveBatch(deliveryID, string(normalized.JobType), normalized.RepoFullName, normalized.PullRequestNumber, normalized.CommitSHA)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: batch identity", ErrFrozenGraphIntent)
	}
	for _, job := range normalized.Jobs {
		expected, err := operation.DeriveJob(batchID, job.ProjectName, job.WorkflowFile)
		if err != nil || job.OperationID != expected.String() {
			return nil, nil, fmt.Errorf("%w: job identity for %q", ErrFrozenGraphIntent, job.ProjectName)
		}
	}
	return normalized, order, nil
}

func normalizeFrozenGraphShape(input DurableGraphIntent) (*DurableGraphIntent, []string, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, nil, ErrFrozenGraphIntent
	}
	var intent DurableGraphIntent
	if err := json.Unmarshal(encoded, &intent); err != nil {
		return nil, nil, ErrFrozenGraphIntent
	}
	switch intent.JobType {
	case scheduler.DiggerCommandNoop, scheduler.DiggerCommandPlan, scheduler.DiggerCommandApply, scheduler.DiggerCommandLock, scheduler.DiggerCommandUnlock:
	default:
		return nil, nil, fmt.Errorf("%w: unknown durable job type %q", ErrFrozenGraphIntent, intent.JobType)
	}
	if len(intent.Jobs) == 0 || strings.TrimSpace(intent.JobReporterType) == "" {
		return nil, nil, fmt.Errorf("%w: jobs and reporter required", ErrFrozenGraphIntent)
	}
	dependencies := graph.New(graph.StringHash, graph.Directed())
	projects := make(map[string]bool, len(intent.Jobs))
	operations := make(map[string]bool, len(intent.Jobs))
	for index := range intent.Jobs {
		job := &intent.Jobs[index]
		if strings.TrimSpace(job.ProjectName) == "" || strings.TrimSpace(job.WorkflowFile) == "" || projects[job.ProjectName] ||
			!operation.ID(job.OperationID).Valid() || operations[job.OperationID] {
			return nil, nil, fmt.Errorf("%w: duplicate or malformed job %q", ErrFrozenGraphIntent, job.ProjectName)
		}
		projects[job.ProjectName], operations[job.OperationID] = true, true
		var spec scheduler.JobJson
		decoder := json.NewDecoder(bytes.NewReader(job.SerializedSpec))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&spec); err != nil {
			return nil, nil, fmt.Errorf("%w: job specification", ErrFrozenGraphIntent)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, nil, fmt.Errorf("%w: trailing job specification", ErrFrozenGraphIntent)
		}
		if spec.ProjectName != job.ProjectName || spec.JobType != string(intent.JobType) ||
			spec.PullRequestNumber == nil || *spec.PullRequestNumber != intent.PullRequestNumber ||
			spec.Commit != intent.CommitSHA || spec.Branch != intent.Branch ||
			spec.BackendJobToken != "" || spec.BackendHostname != "" || spec.BackendOrganisationName != "" ||
			(job.CheckRunID == nil) != (job.CheckRunURL == nil) {
			return nil, nil, fmt.Errorf("%w: job specification mismatch for %q", ErrFrozenGraphIntent, job.ProjectName)
		}
		job.SerializedSpec, err = json.Marshal(spec)
		if err != nil {
			return nil, nil, ErrFrozenGraphIntent
		}
		if job.Parents == nil {
			job.Parents = []string{}
		}
		sort.Strings(job.Parents)
		if err := dependencies.AddVertex(job.ProjectName); err != nil {
			return nil, nil, fmt.Errorf("%w: project", ErrFrozenGraphIntent)
		}
	}
	for _, job := range intent.Jobs {
		for index, parent := range job.Parents {
			if !projects[parent] || parent == job.ProjectName || (index > 0 && parent == job.Parents[index-1]) {
				return nil, nil, fmt.Errorf("%w: dependency %q -> %q", ErrFrozenGraphIntent, parent, job.ProjectName)
			}
			if err := dependencies.AddEdge(parent, job.ProjectName); err != nil {
				return nil, nil, fmt.Errorf("%w: dependency", ErrFrozenGraphIntent)
			}
		}
	}
	order, err := graph.StableTopologicalSort(dependencies, func(a, b string) bool { return a < b })
	if err != nil {
		return nil, nil, fmt.Errorf("%w: cyclic dependencies", ErrFrozenGraphIntent)
	}
	sort.Slice(intent.Jobs, func(i, j int) bool { return intent.Jobs[i].ProjectName < intent.Jobs[j].ProjectName })
	return &intent, order, nil
}
