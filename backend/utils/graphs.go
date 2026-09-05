package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dchest/uniuri"
	"github.com/diggerhq/digger/backend/models"
	configuration "github.com/diggerhq/digger/libs/digger_config"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/dominikbraun/graph"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDurableJobGraphConflict = errors.New("durable job graph already exists with different state")
var ErrDurableJobGraphClaim = errors.New("durable job graph delivery claim is not owned by this writer")
var ErrDurableJobGraphTenant = errors.New("durable job graph tenant does not match the claimed delivery")

type DurableJobGraphRequest struct {
	Identity                 models.JobCreationIdentity
	JobType                  scheduler.DiggerCommand
	JobReporterType          string
	OrganisationID           uint
	Jobs                     map[string]scheduler.Job
	Projects                 map[string]configuration.Project
	ProjectsGraph            graph.Graph[string, configuration.Project]
	GithubInstallationID     int64
	Branch                   string
	PullRequestNumber        int
	RepoOwner                string
	RepoName                 string
	RepoFullName             string
	CommitSHA                string
	CommentID                *int64
	DiggerConfig             string
	AISummaryCommentID       string
	ReportTerraformOutput    bool
	CoverAllImpactedProjects bool
	VCSConnectionID          *uint
	BatchCheckRunData        *CheckRunData
	JobCheckRunDataByProject map[string]CheckRunData
}

type durablePreparedJob struct {
	projectName            string
	operationID            operation.ID
	serializedSpec         []byte
	intentSpec             []byte
	dependencyOperationIDs []byte
	tokenValue             string
	workflowFile           string
	checkRunID             *string
	checkRunURL            *string
}

// ConvertJobsToDiggerJobs jobs is map with project name as a key and a Job as a value
func ConvertJobsToDiggerJobs(jobType scheduler.DiggerCommand, jobReporterType string, vcsType models.DiggerVCSType, organisationId uint, jobsMap map[string]scheduler.Job, projectMap map[string]configuration.Project, projectsGraph graph.Graph[string, configuration.Project], githubInstallationId int64, branch string, prNumber int, repoOwner string, repoName string, repoFullName string, commitSha string, commentId *int64, diggerConfigStr string, gitlabProjectId int, aiSummaryCommentId string, reportTerraformOutput bool, coverAllImpactedProjects bool, VCSConnectionId *uint, batchCheckRunData *CheckRunData, jobsCheckRunIdsMap map[string]CheckRunData) (*uuid.UUID, map[string]*models.DiggerJob, error) {
	slog.Info("Converting jobs to Digger jobs",
		"jobType", jobType,
		"vcsType", vcsType,
		"organisationId", organisationId,
		"jobCount", len(jobsMap),
		slog.Group("repository",
			slog.String("fullName", repoFullName),
			slog.String("owner", repoOwner),
			slog.String("name", repoName),
		),
		"branch", branch,
		"prNumber", prNumber,
	)

	result := make(map[string]*models.DiggerJob)
	organisation, err := models.DB.GetOrganisationById(organisationId)
	if err != nil {
		slog.Error("Failed to get organisation",
			"organisationId", organisationId,
			"error", err,
		)
		return nil, nil, fmt.Errorf("error retrieving organisation")
	}
	organisationName := organisation.Name

	backendHostName := GetPublicBaseURL()

	slog.Debug("Processing jobs", "count", len(jobsMap))
	marshalledJobsMap := map[string][]byte{}
	for projectName, job := range jobsMap {
		jobToken, err := models.DB.CreateDiggerJobToken(organisationId)
		if err != nil {
			slog.Error("Failed to create job token",
				"projectName", projectName,
				"organisationId", organisationId,
				"error", err,
			)
			return nil, nil, fmt.Errorf("error creating job token")
		}

		marshalled, err := json.Marshal(scheduler.JobToJson(job, jobType, organisationName, branch, commitSha, jobToken.Value, backendHostName, projectMap[projectName]))
		if err != nil {
			slog.Error("Failed to marshal job",
				"projectName", projectName,
				"error", err,
			)
			return nil, nil, err
		}
		marshalledJobsMap[job.ProjectName] = marshalled

		slog.Debug("Marshalled job",
			"projectName", job.ProjectName,
			"dataLength", len(marshalled),
		)
	}

	var batchCheckRunId *string = nil
	var batchCheckRunUrl *string = nil
	if batchCheckRunData != nil {
		batchCheckRunId = &batchCheckRunData.Id
		batchCheckRunUrl = &batchCheckRunData.Url
	}
	batch, err := models.DB.CreateDiggerBatch(vcsType, githubInstallationId, repoOwner, repoName, repoFullName, prNumber, diggerConfigStr, branch, jobType, commentId, gitlabProjectId, aiSummaryCommentId, reportTerraformOutput, coverAllImpactedProjects, VCSConnectionId, commitSha, batchCheckRunId, batchCheckRunUrl)
	if err != nil {
		slog.Error("Failed to create batch", "error", err)
		return nil, nil, fmt.Errorf("failed to create batch: %v", err)
	}

	slog.Debug("Created batch", "batchId", batch.ID)

	graphWithImpactedProjectsOnly, err := ImpactedProjectsOnlyGraph(projectsGraph, projectMap)
	if err != nil {
		slog.Error("Failed to create impacted projects graph", "error", err)
		return nil, nil, err
	}

	predecessorMap, err := graphWithImpactedProjectsOnly.PredecessorMap()
	if err != nil {
		slog.Error("Failed to get predecessor map", "error", err)
		return nil, nil, err
	}

	visit := func(value string) bool {
		var jobCheckRunId *string = nil
		var jobCheckRunUrl *string = nil
		if jobsCheckRunIdsMap != nil {
			if v, ok := jobsCheckRunIdsMap[value]; ok {
				jobCheckRunId = &v.Id
				jobCheckRunUrl = &v.Url
			}
		}
		if predecessorMap[value] == nil || len(predecessorMap[value]) == 0 {
			slog.Debug("Processing node with no parents", "projectName", value)
			parentJob, err := models.DB.CreateDiggerJob(batch.ID, marshalledJobsMap[value], projectMap[value].WorkflowFile, jobCheckRunId, jobCheckRunUrl, jobReporterType, value)
			if err != nil {
				slog.Error("Failed to create job",
					"projectName", value,
					"batchId", batch.ID,
					"error", err,
				)
				return false
			}

			_, err = models.DB.CreateDiggerJobLink(parentJob.DiggerJobID, repoFullName)
			if err != nil {
				slog.Error("Failed to create job link",
					"jobId", parentJob.DiggerJobID,
					"repoFullName", repoFullName,
					"error", err,
				)
				return false
			}

			result[value] = parentJob
			slog.Debug("Created job with no parents",
				"projectName", value,
				"jobId", parentJob.DiggerJobID,
			)
			return false
		} else {
			parents := predecessorMap[value]
			slog.Debug("Processing node with parents",
				"projectName", value,
				"parentCount", len(parents),
			)

			for _, edge := range parents {
				parent := edge.Source
				parentDiggerJob := result[parent]

				childJob, err := models.DB.CreateDiggerJob(batch.ID, marshalledJobsMap[value], projectMap[value].WorkflowFile, jobCheckRunId, jobCheckRunUrl, jobReporterType, value)
				if err != nil {
					slog.Error("Failed to create child job",
						"projectName", value,
						"parentProject", parent,
						"batchId", batch.ID,
						"error", err,
					)
					return false
				}

				_, err = models.DB.CreateDiggerJobLink(childJob.DiggerJobID, repoFullName)
				if err != nil {
					slog.Error("Failed to create job link",
						"jobId", childJob.DiggerJobID,
						"repoFullName", repoFullName,
						"error", err,
					)
					return false
				}

				err = models.DB.CreateDiggerJobParentLink(parentDiggerJob.DiggerJobID, childJob.DiggerJobID)
				if err != nil {
					slog.Error("Failed to create job parent link",
						"parentJobId", parentDiggerJob.DiggerJobID,
						"childJobId", childJob.DiggerJobID,
						"error", err,
					)
					return false
				}

				result[value] = childJob
				slog.Debug("Created job with parent",
					"projectName", value,
					"jobId", childJob.DiggerJobID,
					"parentProject", parent,
					"parentJobId", parentDiggerJob.DiggerJobID,
				)
			}
			return false
		}
	}

	err = TraverseGraphVisitAllParentsFirst(graphWithImpactedProjectsOnly, visit)
	if err != nil {
		slog.Error("Failed to traverse graph", "error", err)
		return nil, nil, err
	}

	slog.Info("Successfully converted jobs to Digger jobs",
		"batchId", batch.ID,
		"diggerJobCount", len(result),
	)

	return &batch.ID, result, nil
}

// ConvertJobsToDiggerJobsDurable persists one delivery's batch, jobs, tokens,
// provider links, and dependency links in the same fenced transaction. A retry
// returns the previously committed graph after validating its immutable shape.
func ConvertJobsToDiggerJobsDurable(ctx context.Context, request DurableJobGraphRequest) (*uuid.UUID, map[string]*models.DiggerJob, error) {
	intent, err := PrepareDurableGraphIntent(request)
	if err != nil {
		return nil, nil, err
	}
	return CreateDurableGraphFromIntent(ctx, request.Identity, *intent)
}

// PrepareDurableGraphIntent freezes the selected execution inputs without
// accessing the database, generating credentials, or creating runtime providers.
func PrepareDurableGraphIntent(request DurableJobGraphRequest) (*models.DurableGraphIntent, error) {
	if strings.TrimSpace(request.Identity.DatabaseIdentity) == "" || request.Identity.WriterEpoch <= 0 {
		return nil, models.ErrControlPlaneUnconfigured
	}
	if strings.TrimSpace(request.Identity.DeliveryLeaseID) == "" {
		return nil, ErrDurableJobGraphClaim
	}
	if request.Identity.ProtocolVersion != operation.ProtocolVersion {
		return nil, fmt.Errorf("durable job graph protocol version %d does not match binary version %d", request.Identity.ProtocolVersion, operation.ProtocolVersion)
	}
	deliveryOperationID := operation.ID(request.Identity.DeliveryOperationID)
	if !deliveryOperationID.Valid() {
		return nil, fmt.Errorf("invalid delivery operation identity")
	}
	if len(request.Jobs) == 0 {
		return nil, fmt.Errorf("durable job graph must contain at least one job")
	}
	for project := range request.JobCheckRunDataByProject {
		if _, exists := request.Jobs[project]; !exists {
			return nil, fmt.Errorf("check-run metadata references unselected project %q", project)
		}
	}
	if request.ProjectsGraph == nil {
		return nil, fmt.Errorf("durable project graph is required")
	}

	impactedGraph, err := durableImpactedProjectsOnlyGraph(request.ProjectsGraph, request.Projects)
	if err != nil {
		return nil, fmt.Errorf("create impacted projects graph: %w", err)
	}
	projectOrder, err := graph.StableTopologicalSort(impactedGraph, func(first string, second string) bool {
		return first < second
	})
	if err != nil {
		return nil, fmt.Errorf("sort impacted projects graph: %w", err)
	}
	predecessorMap, err := impactedGraph.PredecessorMap()
	if err != nil {
		return nil, fmt.Errorf("get impacted project predecessors: %w", err)
	}
	if len(projectOrder) != len(request.Jobs) || len(request.Projects) != len(request.Jobs) {
		return nil, fmt.Errorf("durable job, project, and graph sets must match")
	}

	batchOperationID, err := operation.DeriveBatch(deliveryOperationID, string(request.JobType), request.RepoFullName, request.PullRequestNumber, request.CommitSHA)
	if err != nil {
		return nil, fmt.Errorf("derive batch operation identity: %w", err)
	}
	jobOperationIDs := make(map[string]operation.ID, len(projectOrder))
	for _, projectName := range projectOrder {
		job, jobExists := request.Jobs[projectName]
		project, projectExists := request.Projects[projectName]
		if !jobExists || !projectExists || job.ProjectName != projectName || project.Name != projectName ||
			job.PullRequestNumber == nil || *job.PullRequestNumber != request.PullRequestNumber {
			return nil, fmt.Errorf("durable job and project identity mismatch for %q", projectName)
		}
		jobOperationID, deriveErr := operation.DeriveJob(batchOperationID, projectName, project.WorkflowFile)
		if deriveErr != nil {
			return nil, fmt.Errorf("derive job operation identity for %q: %w", projectName, deriveErr)
		}
		jobOperationIDs[projectName] = jobOperationID
	}
	preparedJobs, err := prepareDurableJobs(request, projectOrder, jobOperationIDs, predecessorMap)
	if err != nil {
		return nil, err
	}
	intent := durableGraphIntent(request, preparedJobs, predecessorMap)
	normalized, _, err := models.NormalizeDurableGraphIntent(request.Identity.DeliveryOperationID, intent)
	return normalized, err
}

// CreateDurableGraphFromIntent consumes the frozen execution contract directly.
// A retry reads existing credentials; only a fresh graph creates new tokens.
func CreateDurableGraphFromIntent(ctx context.Context, identity models.JobCreationIdentity, frozen models.DurableGraphIntent) (*uuid.UUID, map[string]*models.DiggerJob, error) {
	if strings.TrimSpace(identity.DatabaseIdentity) == "" || identity.WriterEpoch <= 0 {
		return nil, nil, models.ErrControlPlaneUnconfigured
	}
	if strings.TrimSpace(identity.DeliveryLeaseID) == "" {
		return nil, nil, ErrDurableJobGraphClaim
	}
	if identity.ProtocolVersion != operation.ProtocolVersion || frozen.ProtocolVersion != identity.ProtocolVersion {
		return nil, nil, models.ErrControlPlaneProtocol
	}
	deliveryOperationID := operation.ID(identity.DeliveryOperationID)
	if !deliveryOperationID.Valid() {
		return nil, nil, fmt.Errorf("invalid delivery operation identity")
	}
	intent, err := cloneDurableGraphIntent(frozen)
	if err != nil {
		return nil, nil, err
	}
	batchOperationID, err := operation.DeriveBatch(deliveryOperationID, string(intent.JobType), intent.RepoFullName, intent.PullRequestNumber, intent.CommitSHA)
	if err != nil {
		return nil, nil, err
	}
	preparedJobs, projectOrder, predecessorMap, err := prepareFrozenDurableJobs(*intent, deliveryOperationID)
	if err != nil {
		return nil, nil, err
	}
	request := durableGraphRequestFromIntent(identity, *intent)
	canonicalIntent := durableGraphIntent(request, preparedJobs, predecessorMap)
	graphIntentSHA256, err := canonicalIntent.SHA256()
	if err != nil {
		return nil, nil, err
	}

	var batchID uuid.UUID
	result := make(map[string]*models.DiggerJob, len(projectOrder))
	err = models.DB.WithAuthoritativeWriteTx(ctx, request.Identity.DatabaseIdentity, request.Identity.WriterEpoch, true, func(tx *gorm.DB, _ *models.ControlPlaneFence) error {
		var delivery models.GithubWebhookDelivery
		deliveryQuery := tx
		if tx.Dialector.Name() == "postgres" {
			deliveryQuery = deliveryQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if queryErr := deliveryQuery.First(&delivery, "operation_id = ?", deliveryOperationID.String()).Error; queryErr != nil {
			return fmt.Errorf("load durable webhook delivery: %w", queryErr)
		}
		now, timeErr := durableTransactionNow(tx)
		if timeErr != nil {
			return timeErr
		}
		if delivery.ProcessingStatus != models.GithubWebhookDeliveryProcessing || delivery.LeaseID != request.Identity.DeliveryLeaseID || delivery.WriterEpoch == nil || *delivery.WriterEpoch != request.Identity.WriterEpoch || delivery.LeaseExpiresAt == nil || !delivery.LeaseExpiresAt.After(now) {
			return ErrDurableJobGraphClaim
		}
		if delivery.InstallationID == nil || *delivery.InstallationID <= 0 || strings.TrimSpace(delivery.RepositoryFullName) == "" ||
			request.GithubInstallationID != *delivery.InstallationID || request.RepoFullName != delivery.RepositoryFullName ||
			request.RepoOwner == "" || request.RepoName == "" || request.RepoOwner+"/"+request.RepoName != request.RepoFullName {
			return ErrDurableJobGraphTenant
		}

		var installationLinks []models.GithubAppInstallationLink
		installationLinkQuery := tx.Where(
			"github_installation_id = ? AND status = ?",
			*delivery.InstallationID,
			models.GithubAppInstallationLinkActive,
		)
		if tx.Dialector.Name() == "postgres" {
			installationLinkQuery = installationLinkQuery.Clauses(clause.Locking{Strength: "SHARE"})
		}
		if queryErr := installationLinkQuery.Find(&installationLinks).Error; queryErr != nil {
			return fmt.Errorf("load durable GitHub installation tenant link: %w", queryErr)
		}
		if len(installationLinks) != 1 || installationLinks[0].OrganisationId != request.OrganisationID {
			return ErrDurableJobGraphTenant
		}
		if request.VCSConnectionID != nil {
			var connection models.VCSConnection
			connectionQuery := tx
			if tx.Dialector.Name() == "postgres" {
				connectionQuery = connectionQuery.Clauses(clause.Locking{Strength: "SHARE"})
			}
			if queryErr := connectionQuery.First(&connection, "id = ?", *request.VCSConnectionID).Error; queryErr != nil {
				if errors.Is(queryErr, gorm.ErrRecordNotFound) {
					return ErrDurableJobGraphTenant
				}
				return fmt.Errorf("load durable GitHub app connection: %w", queryErr)
			}
			if connection.OrganisationID != request.OrganisationID || connection.GithubId != delivery.GithubAppID || connection.VCSType != models.DiggerVCSGithub {
				return ErrDurableJobGraphTenant
			}
		}

		var organisation models.Organisation
		organisationQuery := tx
		if tx.Dialector.Name() == "postgres" {
			organisationQuery = organisationQuery.Clauses(clause.Locking{Strength: "SHARE"})
		}
		if queryErr := organisationQuery.First(&organisation, "id = ?", installationLinks[0].OrganisationId).Error; queryErr != nil {
			return fmt.Errorf("load durable job organisation: %w", queryErr)
		}
		now, timeErr = durableTransactionNow(tx)
		if timeErr != nil {
			return timeErr
		}
		if delivery.LeaseExpiresAt == nil || !delivery.LeaseExpiresAt.After(now) {
			return ErrDurableJobGraphClaim
		}
		var existingBatchOperation models.ControlOperation
		existingOperationErr := tx.First(&existingBatchOperation, "delivery_id = ? AND operation_kind = ?", delivery.DeliveryID, "digger_batch").Error
		if existingOperationErr == nil {
			if existingBatchOperation.OperationID != batchOperationID.String() || existingBatchOperation.IdentitySHA256 != graphIntentSHA256 ||
				existingBatchOperation.GithubDeliveryID == nil || *existingBatchOperation.GithubDeliveryID != delivery.DeliveryID ||
				existingBatchOperation.ProtocolVersion != request.Identity.ProtocolVersion {
				return ErrDurableJobGraphConflict
			}
			var existingBatch models.DiggerBatch
			if existingErr := tx.Unscoped().First(&existingBatch, "operation_id = ?", existingBatchOperation.OperationID).Error; existingErr != nil {
				return fmt.Errorf("load existing durable batch: %w", existingErr)
			}
			existingJobs, loadErr := loadExistingDurableJobGraph(tx, &existingBatch, &existingBatchOperation, batchOperationID, request, preparedJobs, predecessorMap)
			if loadErr != nil {
				return loadErr
			}
			batchID = existingBatch.ID
			result = existingJobs
			return nil
		}
		if !errors.Is(existingOperationErr, gorm.ErrRecordNotFound) {
			return fmt.Errorf("find durable batch operation: %w", existingOperationErr)
		}
		for index := range preparedJobs {
			var spec scheduler.JobJson
			if err := json.Unmarshal(preparedJobs[index].intentSpec, &spec); err != nil {
				return err
			}
			preparedJobs[index].tokenValue = "cli:" + uuid.NewString()
			spec.BackendJobToken = preparedJobs[index].tokenValue
			spec.BackendHostname = GetPublicBaseURL()
			spec.BackendOrganisationName = organisation.Name
			preparedJobs[index].serializedSpec, err = json.Marshal(spec)
			if err != nil {
				return err
			}
		}

		deliveryID := delivery.DeliveryID
		batchOperation := models.ControlOperation{
			OperationID:      batchOperationID.String(),
			OperationKind:    "digger_batch",
			IdentitySHA256:   graphIntentSHA256,
			GithubDeliveryID: &deliveryID,
			WriterEpoch:      request.Identity.WriterEpoch,
			ProtocolVersion:  request.Identity.ProtocolVersion,
			Status:           models.ControlOperationPending,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		if createErr := tx.Omit("Delivery").Create(&batchOperation).Error; createErr != nil {
			return fmt.Errorf("create durable batch operation: %w", createErr)
		}

		batchOperationValue := batchOperationID.String()
		writerEpoch := request.Identity.WriterEpoch
		batch := models.DiggerBatch{
			ID:                       uuid.New(),
			OperationID:              &batchOperationValue,
			ProtocolVersion:          request.Identity.ProtocolVersion,
			WriterEpoch:              &writerEpoch,
			DiggerBatchID:            uniuri.NewLen(7),
			VCS:                      models.DiggerVCSGithub,
			VCSConnectionId:          request.VCSConnectionID,
			GithubInstallationId:     request.GithubInstallationID,
			RepoOwner:                request.RepoOwner,
			RepoName:                 request.RepoName,
			RepoFullName:             request.RepoFullName,
			PrNumber:                 request.PullRequestNumber,
			CommitSha:                request.CommitSHA,
			CommentId:                request.CommentID,
			Status:                   scheduler.BatchJobCreated,
			BranchName:               request.Branch,
			DiggerConfig:             request.DiggerConfig,
			BatchType:                request.JobType,
			AiSummaryCommentId:       request.AISummaryCommentID,
			ReportTerraformOutputs:   request.ReportTerraformOutput,
			CoverAllImpactedProjects: request.CoverAllImpactedProjects,
		}
		if request.BatchCheckRunData != nil {
			batch.CheckRunId = &request.BatchCheckRunData.Id
			batch.CheckRunUrl = &request.BatchCheckRunData.Url
		}
		if createErr := tx.Omit("Operation", "VCSConnection").Create(&batch).Error; createErr != nil {
			return fmt.Errorf("create durable batch: %w", createErr)
		}

		for _, preparedJob := range preparedJobs {
			jobOperationValue := preparedJob.operationID.String()
			summary := models.DiggerJobSummary{}
			if createErr := tx.Create(&summary).Error; createErr != nil {
				return fmt.Errorf("create durable job summary for %q: %w", preparedJob.projectName, createErr)
			}
			batchIDString := batch.ID.String()
			workflowURL := "#"
			job := models.DiggerJob{
				DiggerJobID:            uniuri.NewLen(10),
				OperationID:            &jobOperationValue,
				ProtocolVersion:        request.Identity.ProtocolVersion,
				WriterEpoch:            &writerEpoch,
				Status:                 scheduler.DiggerJobCreated,
				ProjectName:            preparedJob.projectName,
				BatchID:                &batchIDString,
				CheckRunId:             preparedJob.checkRunID,
				CheckRunUrl:            preparedJob.checkRunURL,
				SerializedJobSpec:      preparedJob.serializedSpec,
				DependencyOperationIDs: preparedJob.dependencyOperationIDs,
				DiggerJobSummaryID:     summary.ID,
				DiggerJobSummary:       summary,
				WorkflowRunUrl:         &workflowURL,
				WorkflowFile:           preparedJob.workflowFile,
				ReporterType:           request.JobReporterType,
			}
			jobIntentSHA256, digestErr := models.DurableJobIntentSHA256(&job)
			if digestErr != nil {
				return fmt.Errorf("digest durable job %q: %w", preparedJob.projectName, digestErr)
			}
			jobOperation := models.ControlOperation{
				OperationID:      jobOperationValue,
				OperationKind:    "digger_job",
				IdentitySHA256:   jobIntentSHA256,
				GithubDeliveryID: &deliveryID,
				WriterEpoch:      request.Identity.WriterEpoch,
				ProtocolVersion:  request.Identity.ProtocolVersion,
				Status:           models.ControlOperationPending,
				CreatedAt:        now,
				UpdatedAt:        now,
			}
			if createErr := tx.Omit("Delivery").Create(&jobOperation).Error; createErr != nil {
				return fmt.Errorf("create durable job operation for %q: %w", preparedJob.projectName, createErr)
			}
			if createErr := tx.Omit("Operation", "Batch", "DiggerJobSummary").Create(&job).Error; createErr != nil {
				return fmt.Errorf("create durable job for %q: %w", preparedJob.projectName, createErr)
			}

			jobDatabaseID := job.ID
			jobToken := models.JobToken{
				Value:               preparedJob.tokenValue,
				Expiry:              now,
				OrganisationID:      request.OrganisationID,
				DiggerJobDatabaseID: &jobDatabaseID,
				Type:                models.CliJobAccessType,
			}
			if createErr := tx.Omit("Organisation", "DiggerJob").Create(&jobToken).Error; createErr != nil {
				return fmt.Errorf("create durable job token for %q: %w", preparedJob.projectName, createErr)
			}
			link := models.GithubDiggerJobLink{Status: models.DiggerJobLinkCreated, DiggerJobId: job.DiggerJobID, RepoFullName: request.RepoFullName}
			if createErr := tx.Omit("DiggerJob").Create(&link).Error; createErr != nil {
				return fmt.Errorf("create durable job link for %q: %w", preparedJob.projectName, createErr)
			}
			if len(predecessorMap[preparedJob.projectName]) == 0 {
				dispatchPayload, marshalErr := json.Marshal(models.GithubWorkflowDispatchPayload{
					OperationID: jobOperationValue,
					DiggerJobID: job.DiggerJobID,
				})
				if marshalErr != nil {
					return fmt.Errorf("marshal durable workflow dispatch for %q: %w", preparedJob.projectName, marshalErr)
				}
				effect := models.NewOutboxEffect(
					jobOperationValue,
					models.GithubWorkflowDispatchEffectKind,
					"job:"+jobOperationValue,
					dispatchPayload,
					request.Identity.WriterEpoch,
					now,
				)
				if _, created, enqueueErr := models.EnqueueOutboxEffectTx(tx, effect); enqueueErr != nil {
					return fmt.Errorf("enqueue durable workflow dispatch for %q: %w", preparedJob.projectName, enqueueErr)
				} else if !created {
					return ErrDurableJobGraphConflict
				}
			}
			result[preparedJob.projectName] = &job
		}

		for _, projectName := range projectOrder {
			parentNames := durableParentNames(predecessorMap[projectName])
			for _, parentName := range parentNames {
				parentJob, parentExists := result[parentName]
				childJob, childExists := result[projectName]
				if !parentExists || !childExists {
					return fmt.Errorf("durable dependency %q -> %q does not reference persisted jobs", parentName, projectName)
				}
				parentLink := models.DiggerJobParentLink{ParentDiggerJobId: parentJob.DiggerJobID, DiggerJobId: childJob.DiggerJobID}
				if createErr := tx.Omit("DiggerJob", "ParentDiggerJob").Create(&parentLink).Error; createErr != nil {
					return fmt.Errorf("create durable dependency %q -> %q: %w", parentName, projectName, createErr)
				}
			}
		}

		batchID = batch.ID
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return &batchID, result, nil
}

func prepareDurableJobs(request DurableJobGraphRequest, projectOrder []string, jobOperationIDs map[string]operation.ID, predecessorMap map[string]map[string]graph.Edge[string]) ([]durablePreparedJob, error) {
	preparedJobs := make([]durablePreparedJob, 0, len(projectOrder))
	for _, projectName := range projectOrder {
		job := request.Jobs[projectName]
		project := request.Projects[projectName]
		intentSpec := scheduler.JobToJson(job, request.JobType, "", request.Branch, request.CommitSHA, "", "", project)
		serializedIntentSpec, err := json.Marshal(intentSpec)
		if err != nil {
			return nil, fmt.Errorf("marshal durable job intent %q: %w", projectName, err)
		}
		parentOperationIDs := make([]string, 0, len(predecessorMap[projectName]))
		for parentName := range predecessorMap[projectName] {
			parentOperationID, ok := jobOperationIDs[parentName]
			if !ok {
				return nil, fmt.Errorf("missing durable parent operation for %q", parentName)
			}
			parentOperationIDs = append(parentOperationIDs, parentOperationID.String())
		}
		sort.Strings(parentOperationIDs)
		serializedParentOperationIDs, err := json.Marshal(parentOperationIDs)
		if err != nil {
			return nil, fmt.Errorf("marshal durable parent operations for %q: %w", projectName, err)
		}
		prepared := durablePreparedJob{
			projectName:            projectName,
			operationID:            jobOperationIDs[projectName],
			serializedSpec:         serializedIntentSpec,
			intentSpec:             serializedIntentSpec,
			dependencyOperationIDs: serializedParentOperationIDs,
			workflowFile:           project.WorkflowFile,
		}
		if checkRunData, ok := request.JobCheckRunDataByProject[projectName]; ok {
			prepared.checkRunID = &checkRunData.Id
			prepared.checkRunURL = &checkRunData.Url
		}
		preparedJobs = append(preparedJobs, prepared)
	}
	return preparedJobs, nil
}

func durableGraphIntent(request DurableJobGraphRequest, preparedJobs []durablePreparedJob, predecessorMap map[string]map[string]graph.Edge[string]) models.DurableGraphIntent {
	var batchCheckRunData *models.DurableGraphCheckRunData
	if request.BatchCheckRunData != nil {
		batchCheckRunData = &models.DurableGraphCheckRunData{Id: request.BatchCheckRunData.Id, Url: request.BatchCheckRunData.Url}
	}
	intent := models.DurableGraphIntent{
		ProtocolVersion:          request.Identity.ProtocolVersion,
		JobType:                  request.JobType,
		JobReporterType:          request.JobReporterType,
		OrganisationID:           request.OrganisationID,
		GithubInstallationID:     request.GithubInstallationID,
		Branch:                   request.Branch,
		PullRequestNumber:        request.PullRequestNumber,
		RepoOwner:                request.RepoOwner,
		RepoName:                 request.RepoName,
		RepoFullName:             request.RepoFullName,
		CommitSHA:                request.CommitSHA,
		CommentID:                request.CommentID,
		DiggerConfig:             request.DiggerConfig,
		AISummaryCommentID:       request.AISummaryCommentID,
		ReportTerraformOutput:    request.ReportTerraformOutput,
		CoverAllImpactedProjects: request.CoverAllImpactedProjects,
		VCSConnectionID:          request.VCSConnectionID,
		BatchCheckRunData:        batchCheckRunData,
		Jobs:                     make([]models.DurableGraphJobIntent, 0, len(preparedJobs)),
	}
	for _, preparedJob := range preparedJobs {
		intent.Jobs = append(intent.Jobs, models.DurableGraphJobIntent{
			ProjectName:    preparedJob.projectName,
			OperationID:    preparedJob.operationID.String(),
			SerializedSpec: preparedJob.intentSpec,
			WorkflowFile:   preparedJob.workflowFile,
			CheckRunID:     preparedJob.checkRunID,
			CheckRunURL:    preparedJob.checkRunURL,
			Parents:        durableParentNames(predecessorMap[preparedJob.projectName]),
		})
	}
	return intent
}

func cloneDurableGraphIntent(intent models.DurableGraphIntent) (*models.DurableGraphIntent, error) {
	encoded, err := json.Marshal(intent)
	if err != nil {
		return nil, err
	}
	var frozen models.DurableGraphIntent
	if err := json.Unmarshal(encoded, &frozen); err != nil {
		return nil, err
	}
	return &frozen, nil
}

func durableGraphRequestFromIntent(identity models.JobCreationIdentity, intent models.DurableGraphIntent) DurableJobGraphRequest {
	request := DurableJobGraphRequest{
		Identity: identity, JobType: intent.JobType, JobReporterType: intent.JobReporterType,
		OrganisationID: intent.OrganisationID, GithubInstallationID: intent.GithubInstallationID,
		Branch: intent.Branch, PullRequestNumber: intent.PullRequestNumber, RepoOwner: intent.RepoOwner,
		RepoName: intent.RepoName, RepoFullName: intent.RepoFullName, CommitSHA: intent.CommitSHA,
		CommentID: intent.CommentID, DiggerConfig: intent.DiggerConfig, AISummaryCommentID: intent.AISummaryCommentID,
		ReportTerraformOutput: intent.ReportTerraformOutput, CoverAllImpactedProjects: intent.CoverAllImpactedProjects,
		VCSConnectionID: intent.VCSConnectionID,
	}
	if intent.BatchCheckRunData != nil {
		request.BatchCheckRunData = &CheckRunData{Id: intent.BatchCheckRunData.Id, Url: intent.BatchCheckRunData.Url}
	}
	return request
}

func prepareFrozenDurableJobs(intent models.DurableGraphIntent, deliveryOperationID operation.ID) ([]durablePreparedJob, []string, map[string]map[string]graph.Edge[string], error) {
	normalized, order, err := models.NormalizeDurableGraphIntent(deliveryOperationID.String(), intent)
	if err != nil {
		return nil, nil, nil, err
	}
	byProject := make(map[string]durablePreparedJob, len(normalized.Jobs))
	predecessors := make(map[string]map[string]graph.Edge[string], len(normalized.Jobs))
	for _, job := range normalized.Jobs {
		byProject[job.ProjectName] = durablePreparedJob{projectName: job.ProjectName, operationID: operation.ID(job.OperationID),
			serializedSpec: job.SerializedSpec, intentSpec: job.SerializedSpec, workflowFile: job.WorkflowFile,
			checkRunID: job.CheckRunID, checkRunURL: job.CheckRunURL}
		predecessors[job.ProjectName] = make(map[string]graph.Edge[string], len(job.Parents))
		for _, parent := range job.Parents {
			predecessors[job.ProjectName][parent] = graph.Edge[string]{Source: parent, Target: job.ProjectName}
		}
	}
	prepared := make([]durablePreparedJob, 0, len(order))
	for _, name := range order {
		job := byProject[name]
		parentIDs := make([]string, 0, len(predecessors[name]))
		for parent := range predecessors[name] {
			parentIDs = append(parentIDs, byProject[parent].operationID.String())
		}
		sort.Strings(parentIDs)
		job.dependencyOperationIDs, err = json.Marshal(parentIDs)
		if err != nil {
			return nil, nil, nil, err
		}
		prepared = append(prepared, job)
	}
	return prepared, order, predecessors, nil
}

func loadExistingDurableJobGraph(tx *gorm.DB, batch *models.DiggerBatch, batchOperation *models.ControlOperation, batchOperationID operation.ID, request DurableJobGraphRequest, expectedJobs []durablePreparedJob, predecessorMap map[string]map[string]graph.Edge[string]) (map[string]*models.DiggerJob, error) {
	if batch.DeletedAt.Valid || batch.OperationID == nil || *batch.OperationID != batchOperationID.String() || batch.VCS != models.DiggerVCSGithub ||
		batch.WriterEpoch == nil || batchOperation == nil || batchOperation.GithubDeliveryID == nil ||
		batch.ProtocolVersion != batchOperation.ProtocolVersion || *batch.WriterEpoch != batchOperation.WriterEpoch ||
		batch.GithubInstallationId != request.GithubInstallationID || batch.RepoFullName != request.RepoFullName ||
		batch.RepoOwner != request.RepoOwner || batch.RepoName != request.RepoName || batch.PrNumber != request.PullRequestNumber ||
		batch.CommitSha != request.CommitSHA || batch.BatchType != request.JobType || batch.BranchName != request.Branch ||
		batch.DiggerConfig != request.DiggerConfig || !equalOptionalInt64(batch.CommentId, request.CommentID) ||
		batch.AiSummaryCommentId != request.AISummaryCommentID || batch.ReportTerraformOutputs != request.ReportTerraformOutput ||
		batch.CoverAllImpactedProjects != request.CoverAllImpactedProjects || !equalOptionalUint(batch.VCSConnectionId, request.VCSConnectionID) ||
		!equalOptionalString(batch.CheckRunId, checkRunID(request.BatchCheckRunData)) ||
		!equalOptionalString(batch.CheckRunUrl, checkRunURL(request.BatchCheckRunData)) {
		return nil, ErrDurableJobGraphConflict
	}
	var persistedJobs []models.DiggerJob
	if err := tx.Unscoped().Where("batch_id = ?", batch.ID.String()).Find(&persistedJobs).Error; err != nil {
		return nil, fmt.Errorf("load existing durable jobs: %w", err)
	}
	if len(persistedJobs) != len(expectedJobs) {
		return nil, ErrDurableJobGraphConflict
	}

	result := make(map[string]*models.DiggerJob, len(persistedJobs))
	expectedByProject := make(map[string]durablePreparedJob, len(expectedJobs))
	for _, expectedJob := range expectedJobs {
		expectedByProject[expectedJob.projectName] = expectedJob
	}
	for jobIndex := range persistedJobs {
		persistedJob := &persistedJobs[jobIndex]
		expectedJob, expected := expectedByProject[persistedJob.ProjectName]
		persistedDependencyOperationIDs, dependencyErr := models.CanonicalDependencyOperationIDs(persistedJob.DependencyOperationIDs)
		if dependencyErr != nil {
			return nil, ErrDurableJobGraphConflict
		}
		if !expected || persistedJob.DeletedAt.Valid || persistedJob.OperationID == nil || *persistedJob.OperationID != expectedJob.operationID.String() ||
			persistedJob.WorkflowFile != expectedJob.workflowFile || persistedJob.ProtocolVersion != request.Identity.ProtocolVersion ||
			persistedJob.ReporterType != request.JobReporterType || !equalOptionalString(persistedJob.CheckRunId, expectedJob.checkRunID) ||
			!equalOptionalString(persistedJob.CheckRunUrl, expectedJob.checkRunURL) || string(persistedDependencyOperationIDs) != string(expectedJob.dependencyOperationIDs) {
			return nil, ErrDurableJobGraphConflict
		}
		persistedJobIntentSHA256, err := models.DurableJobIntentSHA256(persistedJob)
		if err != nil {
			return nil, ErrDurableJobGraphConflict
		}
		var persistedJobOperation models.ControlOperation
		if err := tx.First(&persistedJobOperation, "operation_id = ?", *persistedJob.OperationID).Error; err != nil {
			return nil, fmt.Errorf("load existing durable job operation for %q: %w", persistedJob.ProjectName, err)
		}
		if persistedJobOperation.OperationKind != "digger_job" || persistedJobOperation.IdentitySHA256 != persistedJobIntentSHA256 ||
			persistedJobOperation.GithubDeliveryID == nil || *persistedJobOperation.GithubDeliveryID != *batchOperation.GithubDeliveryID ||
			persistedJobOperation.ProtocolVersion != batchOperation.ProtocolVersion || persistedJobOperation.WriterEpoch != batchOperation.WriterEpoch || persistedJob.WriterEpoch == nil ||
			*persistedJob.WriterEpoch != persistedJobOperation.WriterEpoch {
			return nil, ErrDurableJobGraphConflict
		}
		var token models.JobToken
		if err := tx.Where("digger_job_database_id = ?", persistedJob.ID).First(&token).Error; err != nil {
			return nil, fmt.Errorf("load existing durable job token for %q: %w", persistedJob.ProjectName, err)
		}
		var persistedSpec scheduler.JobJson
		if err := json.Unmarshal(persistedJob.SerializedJobSpec, &persistedSpec); err != nil {
			return nil, ErrDurableJobGraphConflict
		}
		if token.OrganisationID != request.OrganisationID || token.DiggerJobDatabaseID == nil || *token.DiggerJobDatabaseID != persistedJob.ID || token.Type != models.CliJobAccessType || persistedSpec.BackendJobToken != token.Value {
			return nil, ErrDurableJobGraphConflict
		}
		persistedSpec.BackendJobToken = ""
		persistedSpec.BackendHostname = ""
		persistedSpec.BackendOrganisationName = ""
		var expectedSpec scheduler.JobJson
		if err := json.Unmarshal(expectedJob.serializedSpec, &expectedSpec); err != nil {
			return nil, fmt.Errorf("decode expected durable job spec for %q: %w", expectedJob.projectName, err)
		}
		expectedSpec.BackendJobToken = ""
		expectedSpec.BackendHostname = ""
		expectedSpec.BackendOrganisationName = ""
		persistedSpecJSON, err := json.Marshal(persistedSpec)
		if err != nil {
			return nil, fmt.Errorf("normalize persisted durable job spec for %q: %w", persistedJob.ProjectName, err)
		}
		expectedSpecJSON, err := json.Marshal(expectedSpec)
		if err != nil {
			return nil, fmt.Errorf("normalize expected durable job spec for %q: %w", expectedJob.projectName, err)
		}
		if string(persistedSpecJSON) != string(expectedSpecJSON) {
			return nil, ErrDurableJobGraphConflict
		}
		var jobLinks []models.GithubDiggerJobLink
		if err := tx.Where("digger_job_id = ?", persistedJob.DiggerJobID).Find(&jobLinks).Error; err != nil {
			return nil, fmt.Errorf("load existing durable job links for %q: %w", persistedJob.ProjectName, err)
		}
		if len(jobLinks) != 1 || jobLinks[0].RepoFullName != request.RepoFullName {
			return nil, ErrDurableJobGraphConflict
		}
		var dispatchEffects []models.OutboxEffect
		if err := tx.Where("operation_id = ? AND effect_kind = ?", *persistedJob.OperationID, models.GithubWorkflowDispatchEffectKind).Find(&dispatchEffects).Error; err != nil {
			return nil, fmt.Errorf("load existing durable workflow dispatch for %q: %w", persistedJob.ProjectName, err)
		}
		expectedDispatches := 0
		if len(predecessorMap[persistedJob.ProjectName]) == 0 {
			expectedDispatches = 1
		}
		if len(dispatchEffects) != expectedDispatches {
			return nil, ErrDurableJobGraphConflict
		}
		if expectedDispatches == 1 {
			canonicalPayload, err := json.Marshal(models.GithubWorkflowDispatchPayload{
				OperationID: *persistedJob.OperationID,
				DiggerJobID: persistedJob.DiggerJobID,
			})
			if err != nil {
				return nil, fmt.Errorf("marshal expected durable workflow dispatch for %q: %w", persistedJob.ProjectName, err)
			}
			expectedDigest := sha256.Sum256(canonicalPayload)
			effect := &dispatchEffects[0]
			// WriterEpoch belongs to the current outbox lease, not the original
			// graph identity. Validate immutable fields without allocating a UUID.
			if !effect.ValidPayloadDigest() || effect.ControlOperationID != *persistedJob.OperationID ||
				effect.EffectKind != models.GithubWorkflowDispatchEffectKind || effect.EffectKey != "job:"+*persistedJob.OperationID ||
				effect.PayloadSHA256 != hex.EncodeToString(expectedDigest[:]) {
				return nil, ErrDurableJobGraphConflict
			}
		}
		result[persistedJob.ProjectName] = persistedJob
	}

	expectedParentLinks := make(map[string]struct{})
	for projectName, predecessors := range predecessorMap {
		for _, parentName := range durableParentNames(predecessors) {
			expectedParentLinks[result[parentName].DiggerJobID+"\x00"+result[projectName].DiggerJobID] = struct{}{}
		}
	}
	jobIDs := make([]string, 0, len(result))
	for _, job := range result {
		jobIDs = append(jobIDs, job.DiggerJobID)
	}
	var persistedParentLinks []models.DiggerJobParentLink
	if err := tx.Where("digger_job_id IN ? OR parent_digger_job_id IN ?", jobIDs, jobIDs).Find(&persistedParentLinks).Error; err != nil {
		return nil, fmt.Errorf("load existing durable parent links: %w", err)
	}
	if len(persistedParentLinks) != len(expectedParentLinks) {
		return nil, ErrDurableJobGraphConflict
	}
	for _, parentLink := range persistedParentLinks {
		if _, expected := expectedParentLinks[parentLink.ParentDiggerJobId+"\x00"+parentLink.DiggerJobId]; !expected {
			return nil, ErrDurableJobGraphConflict
		}
	}
	return result, nil
}

func durableParentNames(predecessors map[string]graph.Edge[string]) []string {
	parents := make([]string, 0, len(predecessors))
	for parentName := range predecessors {
		parents = append(parents, parentName)
	}
	sort.Strings(parents)
	return parents
}

func equalOptionalInt64(first *int64, second *int64) bool {
	return (first == nil && second == nil) || (first != nil && second != nil && *first == *second)
}

func equalOptionalUint(first *uint, second *uint) bool {
	return (first == nil && second == nil) || (first != nil && second != nil && *first == *second)
}

func equalOptionalString(first *string, second *string) bool {
	return (first == nil && second == nil) || (first != nil && second != nil && *first == *second)
}

func checkRunID(data *CheckRunData) *string {
	if data == nil {
		return nil
	}
	return &data.Id
}

func checkRunURL(data *CheckRunData) *string {
	if data == nil {
		return nil
	}
	return &data.Url
}

func durableTransactionNow(tx *gorm.DB) (time.Time, error) {
	if tx.Dialector.Name() != "postgres" {
		return time.Now().UTC(), nil
	}
	var now time.Time
	if err := tx.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}
	return now.UTC(), nil
}

func TraverseGraphVisitAllParentsFirst(g graph.Graph[string, configuration.Project], visit func(value string) bool) error {
	slog.Debug("Traversing graph, visiting all parents first")

	// We need a dummy parent node that is ignored during traversal to ensure that all root nodes are visited first,
	// otherwise when looking back at all parents of a node a parent might not be visited yet and we would miss it.
	dummyParent := configuration.Project{Name: "DUMMY_PARENT_PROJECT_FOR_PROCESSING"}
	predecessorMap, err := g.PredecessorMap()
	if err != nil {
		slog.Error("Failed to get predecessor map", "error", err)
		return err
	}

	visitIgnoringDummyParent := func(value string) bool {
		if value == dummyParent.Name {
			return false
		}
		return visit(value)
	}

	err = g.AddVertex(dummyParent)
	if err != nil {
		slog.Error("Failed to add dummy parent vertex", "error", err)
		return err
	}

	rootCount := 0
	for node := range predecessorMap {
		if predecessorMap[node] == nil || len(predecessorMap[node]) == 0 {
			err := g.AddEdge(dummyParent.Name, node)
			if err != nil {
				slog.Error("Failed to add edge from dummy parent",
					"node", node,
					"error", err,
				)
				return err
			}
			rootCount++
		}
	}

	slog.Debug("Added dummy parent to root nodes", "rootNodeCount", rootCount)
	return graph.BFS(g, dummyParent.Name, visitIgnoringDummyParent)
}

func ImpactedProjectsOnlyGraph(projectsGraph graph.Graph[string, configuration.Project], impactedProjectMap map[string]configuration.Project) (graph.Graph[string, configuration.Project], error) {
	slog.Debug("Creating graph with only impacted projects",
		"totalProjects", len(impactedProjectMap),
	)

	adjacencyMap, err := projectsGraph.AdjacencyMap()
	if err != nil {
		slog.Error("Failed to get adjacency map", "error", err)
		return nil, err
	}

	predecessorMap, err := projectsGraph.PredecessorMap()
	if err != nil {
		slog.Error("Failed to get predecessor map", "error", err)
		return nil, err
	}

	graphWithImpactedProjectsOnly := graph.NewLike(projectsGraph)

	rootCount := 0
	for node := range predecessorMap {
		if predecessorMap[node] == nil || len(predecessorMap[node]) == 0 {
			err := CollapsedGraph(nil, node, adjacencyMap, graphWithImpactedProjectsOnly, impactedProjectMap)
			if err != nil {
				slog.Error("Failed to collapse graph",
					"node", node,
					"error", err,
				)
				return nil, err
			}
			rootCount++
		}
	}

	slog.Debug("Created impacted projects graph",
		"rootNodeCount", rootCount,
	)
	return graphWithImpactedProjectsOnly, nil
}

func durableImpactedProjectsOnlyGraph(projectsGraph graph.Graph[string, configuration.Project], impactedProjects map[string]configuration.Project) (graph.Graph[string, configuration.Project], error) {
	adjacencyMap, err := projectsGraph.AdjacencyMap()
	if err != nil {
		return nil, err
	}
	predecessorMap, err := projectsGraph.PredecessorMap()
	if err != nil {
		return nil, err
	}
	result := graph.NewLike(projectsGraph)
	for node, predecessors := range predecessorMap {
		if len(predecessors) == 0 {
			if err := durableCollapsedGraph(nil, node, adjacencyMap, result, impactedProjects); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func durableCollapsedGraph(impactedParent *string, currentNode string, adjMap map[string]map[string]graph.Edge[string], g graph.Graph[string, configuration.Project], impactedProjects map[string]configuration.Project) error {
	// add to the resulting graph only if the project has been impacted by changes
	if _, ok := impactedProjects[currentNode]; ok {
		currentProject, ok := impactedProjects[currentNode]
		if !ok {
			slog.Error("Project not found", "projectName", currentNode)
			return fmt.Errorf("project %s not found", currentNode)
		}

		added := true
		err := g.AddVertex(currentProject)
		if err != nil {
			if errors.Is(err, graph.ErrVertexAlreadyExists) {
				added = false
			} else {
				slog.Error("Failed to add vertex",
					"projectName", currentNode,
					"error", err,
				)
				return err
			}
		}

		if added {
			slog.Debug("Added impacted project to graph", "projectName", currentNode)

			// Process descendants only on the first visit. Later visits still add
			// the additional parent edge without duplicating the subtree walk.
			for child := range adjMap[currentNode] {
				err := durableCollapsedGraph(&currentNode, child, adjMap, g, impactedProjects)
				if err != nil {
					return err
				}
			}
		}

		// if there is an impacted parent add an edge
		if impactedParent != nil {
			err := g.AddEdge(*impactedParent, currentNode)
			if err != nil {
				if errors.Is(err, graph.ErrEdgeAlreadyExists) {
					return nil
				}
				slog.Error("Failed to add edge",
					"parent", *impactedParent,
					"child", currentNode,
					"error", err,
				)
				return err
			}

			slog.Debug("Added edge between impacted projects",
				"parent", *impactedParent,
				"child", currentNode,
			)
		}
	} else {
		// if current wasn't impacted, see children of current node and set currently known parent
		for child := range adjMap[currentNode] {
			err := durableCollapsedGraph(impactedParent, child, adjMap, g, impactedProjects)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func CollapsedGraph(impactedParent *string, currentNode string, adjMap map[string]map[string]graph.Edge[string], g graph.Graph[string, configuration.Project], impactedProjects map[string]configuration.Project) error {
	if _, ok := impactedProjects[currentNode]; ok {
		currentProject, ok := impactedProjects[currentNode]
		if !ok {
			slog.Error("Project not found", "projectName", currentNode)
			return fmt.Errorf("project %s not found", currentNode)
		}

		err := g.AddVertex(currentProject)
		if err != nil {
			if errors.Is(err, graph.ErrVertexAlreadyExists) {
				return nil
			}
			slog.Error("Failed to add vertex", "projectName", currentNode, "error", err)
			return err
		}

		slog.Debug("Added impacted project to graph", "projectName", currentNode)
		for child := range adjMap[currentNode] {
			if err := CollapsedGraph(&currentNode, child, adjMap, g, impactedProjects); err != nil {
				return err
			}
		}
		if impactedParent != nil {
			if err := g.AddEdge(*impactedParent, currentNode); err != nil {
				slog.Error("Failed to add edge", "parent", *impactedParent, "child", currentNode, "error", err)
				return err
			}
			slog.Debug("Added edge between impacted projects", "parent", *impactedParent, "child", currentNode)
		}
	} else {
		for child := range adjMap[currentNode] {
			if err := CollapsedGraph(impactedParent, child, adjMap, g, impactedProjects); err != nil {
				return err
			}
		}
	}
	return nil
}
