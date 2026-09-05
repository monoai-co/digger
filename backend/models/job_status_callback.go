package models

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/diggerhq/digger/libs/iac_utils"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDurableJobStatusCallback = errors.New("durable job status callback is invalid")
var ErrDurableJobStatusCallbackConflict = errors.New("durable job status callback conflicts with persisted state")

type DurableJobStatusCallbackRequest struct {
	CallbackID            uuid.UUID                   `json:"callback_id"`
	RepositoryFullName    string                      `json:"repository_full_name"`
	ProjectName           string                      `json:"project_name"`
	OperationID           string                      `json:"operation_id"`
	ProtocolVersion       int                         `json:"protocol_version"`
	DispatchWriterEpoch   int64                       `json:"dispatch_writer_epoch"`
	TargetStatus          string                      `json:"target_status"`
	ExpectedStatusVersion int64                       `json:"expected_status_version"`
	ClientTimestamp       time.Time                   `json:"client_timestamp"`
	JobSummary            *iac_utils.IacSummary       `json:"job_summary"`
	JobPlanFootprint      *iac_utils.IacPlanFootprint `json:"job_plan_footprint"`
	PRCommentURL          string                      `json:"pr_comment_url"`
	PRCommentID           string                      `json:"pr_comment_id"`
	TerraformOutput       string                      `json:"terraform_output"`
	WorkflowURL           string                      `json:"workflow_url"`
}

type DurableJobStatusCallbackReceipt struct {
	ResponseStatus int
	ResponseBody   []byte
	AlreadyApplied bool
}

func (db *Database) ApplyDurableJobStatusCallback(
	ctx context.Context,
	request DurableJobStatusCallbackRequest,
	diggerJobID string,
	jobTokenValue string,
	executionGrant string,
	databaseIdentity string,
	writerEpoch int64,
) (*DurableJobStatusCallbackReceipt, error) {
	payloadSHA256, err := canonicalDurableStatusCallback(request)
	if err != nil || diggerJobID == "" || jobTokenValue == "" || !validLowerHexDigest(executionGrant, sha256.Size*2) {
		return nil, ErrDurableJobStatusCallback
	}

	var receipt *DurableJobStatusCallbackReceipt
	err = db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, false, func(tx *gorm.DB, fence *ControlPlaneFence) error {
		if err := lockExecutionAdmissionTx(tx); err != nil {
			return err
		}

		var route DiggerJob
		if err := tx.Select("id", "batch_id", "operation_id").First(&route, "operation_id = ? AND digger_job_id = ?", request.OperationID, diggerJobID).Error; err != nil {
			return ErrDurableJobStatusCallbackConflict
		}
		if route.BatchID == nil || route.OperationID == nil {
			return ErrDurableJobStatusCallbackConflict
		}

		var batch DiggerBatch
		batchQuery := tx
		if tx.Dialector.Name() == "postgres" {
			batchQuery = batchQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := batchQuery.First(&batch, "id = ?", *route.BatchID).Error; err != nil {
			return fmt.Errorf("lock durable callback batch: %w", err)
		}

		jobs, operations, tokens, links, err := lockDurableBatchGraphTx(tx, &batch)
		if err != nil {
			return err
		}
		batchOperation, err := validateDurableCallbackGraph(&batch, jobs, operations, tokens, links)
		if err != nil {
			return err
		}
		job := durableJobByID(jobs, route.ID)
		jobOperation := durableOperationByID(operations, request.OperationID)
		token := durableTokenByJobID(tokens, route.ID)
		if job == nil || jobOperation == nil || token == nil || job.BatchID == nil || job.OperationID == nil ||
			*job.BatchID != batch.ID.String() || *job.OperationID != request.OperationID || job.DiggerJobID != diggerJobID ||
			job.ProjectName != request.ProjectName || batch.RepoFullName != request.RepositoryFullName ||
			job.ProtocolVersion != request.ProtocolVersion || batch.ProtocolVersion != request.ProtocolVersion ||
			job.WriterEpoch == nil || *job.WriterEpoch != request.DispatchWriterEpoch ||
			batch.WriterEpoch == nil || *batch.WriterEpoch != request.DispatchWriterEpoch ||
			jobOperation.OperationKind != "digger_job" || jobOperation.ProtocolVersion != request.ProtocolVersion ||
			jobOperation.WriterEpoch != request.DispatchWriterEpoch || token.Type != CliJobAccessType ||
			batchOperation.OperationKind != "digger_batch" ||
			batchOperation.ProtocolVersion != request.ProtocolVersion || batchOperation.WriterEpoch != request.DispatchWriterEpoch ||
			jobOperation.GithubDeliveryID == nil || batchOperation.GithubDeliveryID == nil ||
			*jobOperation.GithubDeliveryID != *batchOperation.GithubDeliveryID {
			return ErrDurableJobStatusCallbackConflict
		}
		var claim ExecutionClaimAttempt
		claimQuery := tx.Where("operation_id = ? AND state = ?", request.OperationID, ExecutionClaimGranted)
		if tx.Dialector.Name() == "postgres" {
			claimQuery = claimQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := claimQuery.First(&claim).Error; err != nil {
			return ErrDurableJobStatusCallbackConflict
		}
		grantDigest := sha256.Sum256([]byte(executionGrant))
		if claim.DiggerJobID != diggerJobID || claim.DiggerJobDatabaseID != job.ID || claim.JobTokenID != token.ID ||
			claim.ProtocolVersion != request.ProtocolVersion || claim.DispatchWriterEpoch != request.DispatchWriterEpoch ||
			!hmac.Equal([]byte(token.Value), []byte(jobTokenValue)) ||
			!hmac.Equal([]byte(claim.GrantTokenSHA256), []byte(hex.EncodeToString(grantDigest[:]))) {
			return ErrDurableJobStatusCallbackConflict
		}

		var existing JobStatusCallback
		existingErr := tx.First(&existing, "callback_id = ?", request.CallbackID).Error
		if existingErr == nil {
			if existing.ControlOperationID != request.OperationID || existing.DiggerJobID != diggerJobID ||
				existing.DiggerJobDatabaseID != job.ID || existing.JobTokenID != token.ID ||
				existing.ExecutionClaimAttemptID != claim.ID || existing.PayloadSHA256 != payloadSHA256 {
				return ErrDurableJobStatusCallbackConflict
			}
			receipt = &DurableJobStatusCallbackReceipt{
				ResponseStatus: existing.ResponseStatus,
				ResponseBody:   append([]byte(nil), existing.ResponseBody...),
				AlreadyApplied: true,
			}
			return nil
		}
		if !errors.Is(existingErr, gorm.ErrRecordNotFound) {
			return existingErr
		}
		// Row-lock waits may outlast the grant. Check current database time only
		// after those locks; committed receipt replay above remains timeless.
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		if token.ActivatedAt == nil || token.RevokedAt != nil || !token.Expiry.After(now) || !claim.GrantExpiresAt.After(now) {
			return ErrDurableJobStatusCallbackConflict
		}

		resultVersion, applied, err := applyDurableStatusTransitionTx(tx, request, job, jobOperation, token, jobs, operations, tokens, now, fence.WriterEpoch)
		if err != nil {
			return err
		}
		if err := updateDurableBatchStateTx(tx, &batch, batchOperation, jobs, now); err != nil {
			return err
		}
		serializedBatch, err := serializeDurableBatchTx(tx, &batch, jobs)
		if err != nil {
			return err
		}
		responseBody, err := json.Marshal(serializedBatch)
		if err != nil {
			return fmt.Errorf("marshal durable callback response: %w", err)
		}
		callback := JobStatusCallback{
			CallbackID:              request.CallbackID,
			ControlOperationID:      request.OperationID,
			DiggerJobID:             diggerJobID,
			DiggerJobDatabaseID:     job.ID,
			JobTokenID:              token.ID,
			ExecutionClaimAttemptID: claim.ID,
			PayloadSHA256:           payloadSHA256,
			TargetStatus:            request.TargetStatus,
			ExpectedStatusVersion:   request.ExpectedStatusVersion,
			ResultStatusVersion:     resultVersion,
			Applied:                 applied,
			ResponseStatus:          200,
			ResponseBody:            responseBody,
			CreatedAt:               now,
		}
		if err := tx.Omit("Operation", "ExactJob", "ExactJobToken", "ExactExecutionClaim").Create(&callback).Error; err != nil {
			return err
		}
		receipt = &DurableJobStatusCallbackReceipt{ResponseStatus: 200, ResponseBody: responseBody}
		return nil
	})
	return receipt, err
}

func canonicalDurableStatusCallback(request DurableJobStatusCallbackRequest) (string, error) {
	if request.CallbackID == uuid.Nil || !operation.ID(request.OperationID).Valid() || request.RepositoryFullName == "" || request.ProjectName == "" ||
		request.ProtocolVersion <= 0 || request.DispatchWriterEpoch <= 0 || request.ExpectedStatusVersion <= 0 || request.ClientTimestamp.IsZero() {
		return "", ErrDurableJobStatusCallback
	}
	switch request.TargetStatus {
	case "started", "succeeded", "failed":
	default:
		return "", ErrDurableJobStatusCallback
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func lockDurableBatchGraphTx(tx *gorm.DB, batch *DiggerBatch) ([]DiggerJob, []ControlOperation, []JobToken, []DiggerJobParentLink, error) {
	var jobs []DiggerJob
	jobQuery := tx.Unscoped().Where("batch_id = ?", batch.ID.String()).Order("operation_id")
	if tx.Dialector.Name() == "postgres" {
		jobQuery = jobQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := jobQuery.Find(&jobs).Error; err != nil {
		return nil, nil, nil, nil, fmt.Errorf("lock durable callback jobs: %w", err)
	}
	if len(jobs) == 0 {
		return nil, nil, nil, nil, ErrDurableJobStatusCallbackConflict
	}
	operationIDs := make([]string, 0, len(jobs)+1)
	if batch.OperationID == nil {
		return nil, nil, nil, nil, ErrDurableJobStatusCallbackConflict
	}
	operationIDs = append(operationIDs, *batch.OperationID)
	jobDatabaseIDs := make([]uint, 0, len(jobs))
	jobPublicIDs := make([]string, 0, len(jobs))
	for index := range jobs {
		if jobs[index].OperationID == nil {
			return nil, nil, nil, nil, ErrDurableJobStatusCallbackConflict
		}
		operationIDs = append(operationIDs, *jobs[index].OperationID)
		jobDatabaseIDs = append(jobDatabaseIDs, jobs[index].ID)
		jobPublicIDs = append(jobPublicIDs, jobs[index].DiggerJobID)
	}
	sort.Strings(operationIDs)
	var operations []ControlOperation
	operationQuery := tx.Where("operation_id IN ?", operationIDs).Order("operation_id")
	if tx.Dialector.Name() == "postgres" {
		operationQuery = operationQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := operationQuery.Find(&operations).Error; err != nil || len(operations) != len(operationIDs) {
		return nil, nil, nil, nil, ErrDurableJobStatusCallbackConflict
	}
	var tokens []JobToken
	tokenQuery := tx.Unscoped().Where("digger_job_database_id IN ?", jobDatabaseIDs).Order("digger_job_database_id")
	if tx.Dialector.Name() == "postgres" {
		tokenQuery = tokenQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := tokenQuery.Find(&tokens).Error; err != nil || len(tokens) != len(jobs) {
		return nil, nil, nil, nil, ErrDurableJobStatusCallbackConflict
	}
	var links []DiggerJobParentLink
	linkQuery := tx.Unscoped().Where("digger_job_id IN ? OR parent_digger_job_id IN ?", jobPublicIDs, jobPublicIDs).Order("digger_job_id, parent_digger_job_id")
	if tx.Dialector.Name() == "postgres" {
		linkQuery = linkQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := linkQuery.Find(&links).Error; err != nil {
		return nil, nil, nil, nil, fmt.Errorf("lock durable callback dependency links: %w", err)
	}
	return jobs, operations, tokens, links, nil
}

func validateDurableCallbackGraph(batch *DiggerBatch, jobs []DiggerJob, operations []ControlOperation, tokens []JobToken, links []DiggerJobParentLink) (*ControlOperation, error) {
	if batch == nil || batch.DeletedAt.Valid || batch.OperationID == nil || batch.WriterEpoch == nil || batch.ProtocolVersion <= 0 ||
		batch.VCS != DiggerVCSGithub || batch.GithubInstallationId <= 0 || batch.RepoOwner == "" || batch.RepoName == "" ||
		batch.RepoOwner+"/"+batch.RepoName != batch.RepoFullName || len(jobs) == 0 || len(operations) != len(jobs)+1 || len(tokens) != len(jobs) {
		return nil, ErrDurableJobStatusCallbackConflict
	}
	if (batch.CheckRunId == nil) != (batch.CheckRunUrl == nil) {
		return nil, ErrDurableJobStatusCallbackConflict
	}

	operationByID := make(map[string]*ControlOperation, len(operations))
	for index := range operations {
		operationRow := &operations[index]
		if operationRow.OperationID == "" || operationByID[operationRow.OperationID] != nil {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		operationByID[operationRow.OperationID] = operationRow
	}
	batchOperation := operationByID[*batch.OperationID]
	if batchOperation == nil || batchOperation.OperationKind != "digger_batch" || batchOperation.GithubDeliveryID == nil ||
		batchOperation.ProtocolVersion != batch.ProtocolVersion || batchOperation.WriterEpoch != *batch.WriterEpoch {
		return nil, ErrDurableJobStatusCallbackConflict
	}

	tokenByJobID := make(map[uint]*JobToken, len(tokens))
	for index := range tokens {
		token := &tokens[index]
		if token.DeletedAt.Valid || token.DiggerJobDatabaseID == nil || tokenByJobID[*token.DiggerJobDatabaseID] != nil {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		tokenByJobID[*token.DiggerJobDatabaseID] = token
	}

	jobByOperationID := make(map[string]*DiggerJob, len(jobs))
	jobByPublicID := make(map[string]*DiggerJob, len(jobs))
	jobIntentByProject := make(map[string]DurableGraphJobIntent, len(jobs))
	organisationID := uint(0)
	jobReporterType := ""
	for index := range jobs {
		job := &jobs[index]
		if job.DeletedAt.Valid || job.ID == 0 || job.DiggerJobID == "" || job.OperationID == nil || job.BatchID == nil ||
			*job.BatchID != batch.ID.String() || job.ProtocolVersion != batch.ProtocolVersion || job.WriterEpoch == nil || *job.WriterEpoch != *batch.WriterEpoch ||
			job.ProjectName == "" || job.WorkflowFile == "" || jobByOperationID[*job.OperationID] != nil || jobByPublicID[job.DiggerJobID] != nil {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		jobOperation := operationByID[*job.OperationID]
		expectedOperationID, deriveErr := operation.DeriveJob(operation.ID(batchOperation.OperationID), job.ProjectName, job.WorkflowFile)
		jobIntentSHA256, digestErr := DurableJobIntentSHA256(job)
		if jobOperation == nil || deriveErr != nil || expectedOperationID.String() != jobOperation.OperationID || digestErr != nil ||
			jobOperation.IdentitySHA256 != jobIntentSHA256 || jobOperation.OperationKind != "digger_job" || jobOperation.GithubDeliveryID == nil ||
			*jobOperation.GithubDeliveryID != *batchOperation.GithubDeliveryID || jobOperation.ProtocolVersion != batch.ProtocolVersion ||
			jobOperation.WriterEpoch != *batch.WriterEpoch {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		token := tokenByJobID[job.ID]
		var jobSpec scheduler.JobJson
		if token == nil || token.OrganisationID == 0 || token.Type != CliJobAccessType || token.Value == "" ||
			json.Unmarshal(job.SerializedJobSpec, &jobSpec) != nil || jobSpec.BackendJobToken != token.Value ||
			jobSpec.ProjectName != job.ProjectName || jobSpec.JobType != string(batch.BatchType) {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		if organisationID == 0 {
			organisationID = token.OrganisationID
		} else if organisationID != token.OrganisationID {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		if jobReporterType == "" {
			jobReporterType = job.ReporterType
		} else if jobReporterType != job.ReporterType {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		jobSpec.BackendJobToken = ""
		jobSpec.BackendHostname = ""
		jobSpec.BackendOrganisationName = ""
		intentSpec, marshalErr := json.Marshal(jobSpec)
		if marshalErr != nil {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		if (job.CheckRunId == nil) != (job.CheckRunUrl == nil) {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		jobIntentByProject[job.ProjectName] = DurableGraphJobIntent{
			ProjectName: job.ProjectName, OperationID: *job.OperationID, SerializedSpec: intentSpec,
			WorkflowFile: job.WorkflowFile, CheckRunID: job.CheckRunId, CheckRunURL: job.CheckRunUrl,
		}
		jobByOperationID[*job.OperationID] = job
		jobByPublicID[job.DiggerJobID] = job
	}
	if len(jobByOperationID) != len(jobs) || len(jobIntentByProject) != len(jobs) || organisationID == 0 || jobReporterType == "" {
		return nil, ErrDurableJobStatusCallbackConflict
	}

	expectedLinks := make(map[string]struct{})
	indegree := make(map[string]int, len(jobs))
	children := make(map[string][]string, len(jobs))
	for index := range jobs {
		job := &jobs[index]
		canonicalDependencies, canonicalErr := CanonicalDependencyOperationIDs(job.DependencyOperationIDs)
		if canonicalErr != nil {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		var dependencyOperationIDs []string
		if json.Unmarshal(canonicalDependencies, &dependencyOperationIDs) != nil {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		parents := make([]string, 0, len(dependencyOperationIDs))
		for _, dependencyOperationID := range dependencyOperationIDs {
			parent := jobByOperationID[dependencyOperationID]
			if parent == nil || parent.ID == job.ID {
				return nil, ErrDurableJobStatusCallbackConflict
			}
			parents = append(parents, parent.ProjectName)
			linkKey := job.DiggerJobID + "\x00" + parent.DiggerJobID
			if _, exists := expectedLinks[linkKey]; exists {
				return nil, ErrDurableJobStatusCallbackConflict
			}
			expectedLinks[linkKey] = struct{}{}
			indegree[job.ProjectName]++
			children[parent.ProjectName] = append(children[parent.ProjectName], job.ProjectName)
		}
		sort.Strings(parents)
		intent := jobIntentByProject[job.ProjectName]
		intent.Parents = parents
		jobIntentByProject[job.ProjectName] = intent
	}

	seenLinks := make(map[string]struct{}, len(links))
	for index := range links {
		link := &links[index]
		if link.DeletedAt.Valid || jobByPublicID[link.DiggerJobId] == nil || jobByPublicID[link.ParentDiggerJobId] == nil {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		linkKey := link.DiggerJobId + "\x00" + link.ParentDiggerJobId
		if _, expected := expectedLinks[linkKey]; !expected {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		if _, duplicate := seenLinks[linkKey]; duplicate {
			return nil, ErrDurableJobStatusCallbackConflict
		}
		seenLinks[linkKey] = struct{}{}
	}
	if len(seenLinks) != len(expectedLinks) {
		return nil, ErrDurableJobStatusCallbackConflict
	}

	ready := make([]string, 0, len(jobs))
	for index := range jobs {
		if indegree[jobs[index].ProjectName] == 0 {
			ready = append(ready, jobs[index].ProjectName)
		}
	}
	sort.Strings(ready)
	orderedIntents := make([]DurableGraphJobIntent, 0, len(jobs))
	for len(ready) > 0 {
		projectName := ready[0]
		ready = ready[1:]
		orderedIntents = append(orderedIntents, jobIntentByProject[projectName])
		for _, childProjectName := range children[projectName] {
			indegree[childProjectName]--
			if indegree[childProjectName] == 0 {
				ready = append(ready, childProjectName)
			}
		}
		sort.Strings(ready)
	}
	if len(orderedIntents) != len(jobs) {
		return nil, ErrDurableJobStatusCallbackConflict
	}

	var batchCheckRunData *DurableGraphCheckRunData
	if batch.CheckRunId != nil {
		batchCheckRunData = &DurableGraphCheckRunData{Id: *batch.CheckRunId, Url: *batch.CheckRunUrl}
	}
	intent := DurableGraphIntent{
		ProtocolVersion: batch.ProtocolVersion, JobType: batch.BatchType, JobReporterType: jobReporterType,
		OrganisationID: organisationID, GithubInstallationID: batch.GithubInstallationId, Branch: batch.BranchName,
		PullRequestNumber: batch.PrNumber, RepoOwner: batch.RepoOwner, RepoName: batch.RepoName, RepoFullName: batch.RepoFullName,
		CommitSHA: batch.CommitSha, CommentID: batch.CommentId, DiggerConfig: batch.DiggerConfig, AISummaryCommentID: batch.AiSummaryCommentId,
		ReportTerraformOutput: batch.ReportTerraformOutputs, CoverAllImpactedProjects: batch.CoverAllImpactedProjects,
		VCSConnectionID: batch.VCSConnectionId, BatchCheckRunData: batchCheckRunData, Jobs: orderedIntents,
	}
	identitySHA256, err := intent.SHA256()
	if err != nil || !hmac.Equal([]byte(identitySHA256), []byte(batchOperation.IdentitySHA256)) {
		return nil, ErrDurableJobStatusCallbackConflict
	}
	return batchOperation, nil
}

func applyDurableStatusTransitionTx(tx *gorm.DB, request DurableJobStatusCallbackRequest, job *DiggerJob, jobOperation *ControlOperation, token *JobToken, jobs []DiggerJob, operations []ControlOperation, tokens []JobToken, now time.Time, writerEpoch int64) (int64, bool, error) {
	if request.ExpectedStatusVersion != 2 || job.Status != scheduler.DiggerJobStarted || job.StatusVersion != request.ExpectedStatusVersion || jobOperation.Status != ControlOperationPending {
		return 0, false, ErrDurableJobStatusCallbackConflict
	}
	if request.TargetStatus == "started" {
		return job.StatusVersion, false, nil
	}

	targetStatus := scheduler.DiggerJobSucceeded
	if request.TargetStatus == "failed" {
		targetStatus = scheduler.DiggerJobFailed
	}
	updates := map[string]any{
		"status":            targetStatus,
		"status_version":    int64(3),
		"status_updated_at": now,
		"pr_comment_url":    request.PRCommentURL,
		"terraform_output":  request.TerraformOutput,
		"updated_at":        now,
	}
	if request.PRCommentID != "" {
		commentID, err := strconv.ParseInt(request.PRCommentID, 10, 64)
		if err != nil || commentID <= 0 {
			return 0, false, ErrDurableJobStatusCallback
		}
		updates["pr_comment_id"] = commentID
	}
	if request.WorkflowURL != "" {
		updates["workflow_run_url"] = request.WorkflowURL
	}
	if request.JobPlanFootprint != nil {
		footprint, err := json.Marshal(request.JobPlanFootprint)
		if err != nil {
			return 0, false, err
		}
		updates["plan_footprint"] = footprint
	}
	result := tx.Model(&DiggerJob{}).Where("id = ? AND status = ? AND status_version = ?", job.ID, scheduler.DiggerJobStarted, request.ExpectedStatusVersion).Updates(updates)
	if result.Error != nil {
		return 0, false, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, false, ErrDurableJobStatusCallbackConflict
	}
	job.Status = targetStatus
	job.StatusVersion = 3
	job.StatusUpdatedAt = now
	job.PRCommentUrl = request.PRCommentURL
	job.TerraformOutput = request.TerraformOutput
	if request.PRCommentID != "" {
		commentID, err := strconv.ParseInt(request.PRCommentID, 10, 64)
		if err != nil || commentID <= 0 {
			return 0, false, ErrDurableJobStatusCallback
		}
		job.PRCommentId = &commentID
	}
	if request.WorkflowURL != "" {
		workflowURL := request.WorkflowURL
		job.WorkflowRunUrl = &workflowURL
	}
	if request.JobPlanFootprint != nil {
		footprint, err := json.Marshal(request.JobPlanFootprint)
		if err != nil {
			return 0, false, err
		}
		job.PlanFootprint = footprint
	}
	if request.JobSummary != nil {
		if err := tx.Model(&DiggerJobSummary{}).Where("id = ?", job.DiggerJobSummaryID).Updates(map[string]any{
			"resources_created": request.JobSummary.ResourcesCreated,
			"resources_updated": request.JobSummary.ResourcesUpdated,
			"resources_deleted": request.JobSummary.ResourcesDeleted,
			"updated_at":        now,
		}).Error; err != nil {
			return 0, false, err
		}
	}
	if err := reconcileDurableTerminalJobTx(tx, job, jobOperation, token, now); err != nil {
		return 0, false, err
	}

	if targetStatus == scheduler.DiggerJobFailed {
		if err := failUnstartedDurableJobsTx(tx, jobs, operations, tokens, now); err != nil {
			return 0, false, err
		}
		return 3, true, nil
	}
	if durableBatchHasFailure(jobs) {
		return 3, true, nil
	}
	if err := enqueueReadyDurableChildrenTx(tx, jobs, now, writerEpoch); err != nil {
		return 0, false, err
	}
	return 3, true, nil
}

func failUnstartedDurableJobsTx(tx *gorm.DB, jobs []DiggerJob, operations []ControlOperation, tokens []JobToken, now time.Time) error {
	for index := range jobs {
		job := &jobs[index]
		if job.Status != scheduler.DiggerJobCreated && job.Status != scheduler.DiggerJobQueuedForRun && job.Status != scheduler.DiggerJobTriggered {
			continue
		}
		operationValue := ""
		if job.OperationID != nil {
			operationValue = *job.OperationID
		}
		operation := durableOperationByID(operations, operationValue)
		token := durableTokenByJobID(tokens, job.ID)
		if operation == nil || token == nil {
			return ErrDurableJobStatusCallbackConflict
		}
		jobResult := tx.Model(&DiggerJob{}).Where("id = ? AND status IN ?", job.ID, []scheduler.DiggerJobStatus{scheduler.DiggerJobCreated, scheduler.DiggerJobQueuedForRun, scheduler.DiggerJobTriggered}).Updates(map[string]any{
			"status": scheduler.DiggerJobFailed, "status_version": int64(3), "status_updated_at": now, "updated_at": now,
		})
		if jobResult.Error != nil {
			return jobResult.Error
		}
		if jobResult.RowsAffected != 1 {
			return ErrDurableJobStatusCallbackConflict
		}
		operationResult := tx.Model(&ControlOperation{}).Where("operation_id = ? AND status = ?", operation.OperationID, ControlOperationPending).Updates(map[string]any{"status": ControlOperationFailed, "updated_at": now})
		if operationResult.Error != nil {
			return operationResult.Error
		}
		if operationResult.RowsAffected != 1 {
			return ErrDurableJobStatusCallbackConflict
		}
		tokenResult := tx.Model(&JobToken{}).Where("id = ? AND digger_job_database_id = ?", token.ID, job.ID).Updates(map[string]any{"expiry": now, "revoked_at": now, "updated_at": now})
		if tokenResult.Error != nil {
			return tokenResult.Error
		}
		if tokenResult.RowsAffected != 1 {
			return ErrDurableJobStatusCallbackConflict
		}
		job.Status = scheduler.DiggerJobFailed
		job.StatusVersion = 3
		operation.Status = ControlOperationFailed
		token.Expiry = now
		token.RevokedAt = &now
	}
	return nil
}

func enqueueReadyDurableChildrenTx(tx *gorm.DB, jobs []DiggerJob, now time.Time, writerEpoch int64) error {
	byOperationID := make(map[string]*DiggerJob, len(jobs))
	for index := range jobs {
		if jobs[index].OperationID == nil {
			return ErrDurableJobStatusCallbackConflict
		}
		byOperationID[*jobs[index].OperationID] = &jobs[index]
	}
	for index := range jobs {
		job := &jobs[index]
		if job.Status != scheduler.DiggerJobCreated {
			continue
		}
		canonicalDependencies, err := CanonicalDependencyOperationIDs(job.DependencyOperationIDs)
		if err != nil {
			return err
		}
		var dependencyIDs []string
		if err := json.Unmarshal(canonicalDependencies, &dependencyIDs); err != nil {
			return err
		}
		if len(dependencyIDs) == 0 {
			continue
		}
		ready := true
		for _, dependencyID := range dependencyIDs {
			parent := byOperationID[dependencyID]
			if parent == nil || parent.Status != scheduler.DiggerJobSucceeded {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		payload, err := json.Marshal(GithubWorkflowDispatchPayload{OperationID: *job.OperationID, DiggerJobID: job.DiggerJobID})
		if err != nil {
			return err
		}
		effect := NewOutboxEffect(*job.OperationID, GithubWorkflowDispatchEffectKind, "job:"+*job.OperationID, payload, writerEpoch, now)
		if _, _, err := EnqueueOutboxEffectTx(tx, effect); err != nil {
			return err
		}
	}
	return nil
}

func updateDurableBatchStateTx(tx *gorm.DB, batch *DiggerBatch, batchOperation *ControlOperation, jobs []DiggerJob, now time.Time) error {
	if batch == nil || batch.OperationID == nil || batchOperation == nil || batchOperation.OperationID != *batch.OperationID || batchOperation.Status != ControlOperationPending {
		return ErrDurableJobStatusCallbackConflict
	}
	targetStatus := scheduler.BatchJobStarted
	allTerminal := true
	failed := false
	for index := range jobs {
		switch jobs[index].Status {
		case scheduler.DiggerJobSucceeded:
		case scheduler.DiggerJobFailed:
			failed = true
		default:
			allTerminal = false
		}
	}
	if allTerminal {
		if failed {
			targetStatus = scheduler.BatchJobFailed
		} else {
			targetStatus = scheduler.BatchJobSucceeded
		}
	}
	if batch.Status != targetStatus {
		result := tx.Model(&DiggerBatch{}).Where("id = ? AND status = ? AND status_version = ?", batch.ID, batch.Status, batch.StatusVersion).Updates(map[string]any{
			"status": targetStatus, "status_version": gorm.Expr("status_version + 1"), "updated_at": now,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrDurableJobStatusCallbackConflict
		}
		batch.Status = targetStatus
		batch.StatusVersion++
	}
	if !allTerminal {
		return nil
	}
	operationStatus := ControlOperationCompleted
	if failed {
		operationStatus = ControlOperationFailed
	}
	result := tx.Model(&ControlOperation{}).Where("operation_id = ? AND status = ?", batchOperation.OperationID, ControlOperationPending).Updates(map[string]any{"status": operationStatus, "updated_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrDurableJobStatusCallbackConflict
	}
	batchOperation.Status = operationStatus
	return nil
}

func serializeDurableBatchTx(tx *gorm.DB, batch *DiggerBatch, lockedJobs []DiggerJob) (*scheduler.SerializedBatch, error) {
	result := &scheduler.SerializedBatch{
		ID: batch.ID.String(), PrNumber: batch.PrNumber, Status: batch.Status, BranchName: batch.BranchName,
		RepoFullName: batch.RepoFullName, RepoOwner: batch.RepoOwner, RepoName: batch.RepoName, BatchType: batch.BatchType,
	}
	if batch.OperationID != nil {
		result.OperationID = *batch.OperationID
	}
	summaries := make(map[uint]DiggerJobSummary, len(lockedJobs))
	summaryIDs := make([]uint, 0, len(lockedJobs))
	for index := range lockedJobs {
		summaryIDs = append(summaryIDs, lockedJobs[index].DiggerJobSummaryID)
	}
	var rows []DiggerJobSummary
	if err := tx.Where("id IN ?", summaryIDs).Find(&rows).Error; err != nil {
		return nil, err
	}
	for index := range rows {
		summaries[rows[index].ID] = rows[index]
	}
	result.Jobs = make([]scheduler.SerializedJob, 0, len(lockedJobs))
	for index := range lockedJobs {
		job := &lockedJobs[index]
		job.DiggerJobSummary = summaries[job.DiggerJobSummaryID]
		serialized, err := job.MapToJsonStruct()
		if err != nil {
			return nil, err
		}
		publicJobSpec, err := json.Marshal(struct {
			JobType      string   `json:"job_type"`
			ProjectName  string   `json:"projectName"`
			ProjectAlias string   `json:"projectAlias"`
			Commands     []string `json:"commands"`
		}{
			JobType:      string(batch.BatchType),
			ProjectName:  serialized.ProjectName,
			ProjectAlias: serialized.ProjectAlias,
			Commands:     []string{"digger " + string(batch.BatchType)},
		})
		if err != nil {
			return nil, fmt.Errorf("marshal public durable callback job spec: %w", err)
		}
		serialized.JobString = publicJobSpec
		result.Jobs = append(result.Jobs, serialized)
	}
	return result, nil
}

func durableJobByID(jobs []DiggerJob, id uint) *DiggerJob {
	for index := range jobs {
		if jobs[index].ID == id {
			return &jobs[index]
		}
	}
	return nil
}

func durableOperationByID(operations []ControlOperation, id string) *ControlOperation {
	for index := range operations {
		if operations[index].OperationID == id {
			return &operations[index]
		}
	}
	return nil
}

func durableTokenByJobID(tokens []JobToken, jobID uint) *JobToken {
	for index := range tokens {
		if tokens[index].DiggerJobDatabaseID != nil && *tokens[index].DiggerJobDatabaseID == jobID {
			return &tokens[index]
		}
	}
	return nil
}

func durableBatchHasFailure(jobs []DiggerJob) bool {
	for index := range jobs {
		if jobs[index].Status == scheduler.DiggerJobFailed {
			return true
		}
	}
	return false
}
