package utils

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type frozenGraphForbiddenRandomness struct{}

func (frozenGraphForbiddenRandomness) Read([]byte) (int, error) {
	return 0, errors.New("graph replay must not generate another credential or identifier")
}

func TestPostgresFrozenGraphIntentReplaysThroughExistingEntryPoint(t *testing.T) {
	t.Setenv("PUBLIC_BASE_URL", "https://source.opentaco.example")
	database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	intent, err := PrepareDurableGraphIntent(request)
	require.NoError(t, err)
	encoded, err := json.Marshal(intent)
	require.NoError(t, err)
	var storedJSON string
	require.NoError(t, database.GormDB.Raw("SELECT CAST(? AS jsonb)::text", string(encoded)).Scan(&storedJSON).Error)
	var frozen models.DurableGraphIntent
	require.NoError(t, json.Unmarshal([]byte(storedJSON), &frozen))
	for _, job := range frozen.Jobs {
		var spec scheduler.JobJson
		require.NoError(t, json.Unmarshal(job.SerializedSpec, &spec))
		require.Empty(t, spec.BackendJobToken)
		require.Empty(t, spec.BackendHostname)
		require.Empty(t, spec.BackendOrganisationName)
	}
	batchID, jobs, err := CreateDurableGraphFromIntent(context.Background(), request.Identity, frozen)
	require.NoError(t, err)
	var originalTokens []models.JobToken
	require.NoError(t, database.GormDB.Order("id").Find(&originalTokens).Error)
	require.Len(t, originalTokens, len(jobs))
	t.Setenv("PUBLIC_BASE_URL", "https://target.opentaco.example")
	uuid.SetRand(frozenGraphForbiddenRandomness{})
	t.Cleanup(func() { uuid.SetRand(rand.Reader) })
	replayID, replayJobs, err := ConvertJobsToDiggerJobsDurable(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, batchID, replayID)
	for name, job := range jobs {
		require.Equal(t, job.ID, replayJobs[name].ID)
		require.Equal(t, job.SerializedJobSpec, replayJobs[name].SerializedJobSpec)
	}
	var replayTokens []models.JobToken
	require.NoError(t, database.GormDB.Order("id").Find(&replayTokens).Error)
	require.Equal(t, originalTokens, replayTokens)
}

func TestFrozenGraphIntentDoesNotAliasPreparationInputs(t *testing.T) {
	_, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	commentID := int64(123)
	request.CommentID = &commentID
	request.BatchCheckRunData = &CheckRunData{Id: "456", Url: "https://github.com/check/456"}
	intent, err := PrepareDurableGraphIntent(request)
	require.NoError(t, err)
	before, err := json.Marshal(intent)
	require.NoError(t, err)
	commentID = 999
	request.BatchCheckRunData.Id = "999"
	for name, job := range request.Jobs {
		job.Commands[0] = "digger destroy"
		job.Teams = []string{"changed-team"}
		request.Jobs[name] = job
	}
	after, err := json.Marshal(intent)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestPostgresFrozenGraphRejectsMalformedIntentBeforeWrites(t *testing.T) {
	mutateSpec := func(change func(*scheduler.JobJson)) func(*models.DurableGraphIntent) {
		return func(intent *models.DurableGraphIntent) {
			var spec scheduler.JobJson
			require.NoError(t, json.Unmarshal(intent.Jobs[0].SerializedSpec, &spec))
			change(&spec)
			encoded, err := json.Marshal(spec)
			require.NoError(t, err)
			intent.Jobs[0].SerializedSpec = encoded
		}
	}
	tests := map[string]func(*models.DurableGraphIntent){
		"empty jobs":        func(intent *models.DurableGraphIntent) { intent.Jobs = nil },
		"duplicate project": func(intent *models.DurableGraphIntent) { intent.Jobs = append(intent.Jobs, intent.Jobs[0]) },
		"wrong operation":   func(intent *models.DurableGraphIntent) { intent.Jobs[0].OperationID = intent.Jobs[1].OperationID },
		"unknown parent":    func(intent *models.DurableGraphIntent) { intent.Jobs[0].Parents = []string{"missing-project"} },
		"self dependency":   func(intent *models.DurableGraphIntent) { intent.Jobs[0].Parents = []string{intent.Jobs[0].ProjectName} },
		"duplicate parent": func(intent *models.DurableGraphIntent) {
			intent.Jobs[0].Parents = []string{intent.Jobs[1].ProjectName, intent.Jobs[1].ProjectName}
		},
		"runtime token":        mutateSpec(func(spec *scheduler.JobJson) { spec.BackendJobToken = "cli:must-not-be-persisted" }),
		"runtime hostname":     mutateSpec(func(spec *scheduler.JobJson) { spec.BackendHostname = "https://unexpected.example" }),
		"runtime organisation": mutateSpec(func(spec *scheduler.JobJson) { spec.BackendOrganisationName = "another-organisation" }),
		"wrong commit":         mutateSpec(func(spec *scheduler.JobJson) { spec.Commit = "different-commit" }),
		"wrong command":        mutateSpec(func(spec *scheduler.JobJson) { spec.JobType = string(scheduler.DiggerCommandApply) }),
		"wrong project spec":   func(intent *models.DurableGraphIntent) { intent.Jobs[0].SerializedSpec = intent.Jobs[1].SerializedSpec },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			database, organisation, delivery := newPostgresDurableGraphTestDatabase(t)
			request := durableGraphTestRequest(t, organisation, delivery)
			intent, err := PrepareDurableGraphIntent(request)
			require.NoError(t, err)
			mutate(intent)
			_, _, err = CreateDurableGraphFromIntent(context.Background(), request.Identity, *intent)
			require.Error(t, err)
			for _, model := range []any{&models.DiggerBatch{}, &models.DiggerJob{}, &models.JobToken{}, &models.OutboxEffect{}} {
				var count int64
				require.NoError(t, database.GormDB.Model(model).Count(&count).Error)
				require.Zero(t, count)
			}
		})
	}
}
