package utils

import (
	"encoding/json"
	"testing"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/diggerhq/digger/libs/scheduler"
	"github.com/stretchr/testify/require"
)

func TestPrepareDurableGraphIntentDoesNotAccessDatabaseOrGenerateCredentials(t *testing.T) {
	database, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	models.DB = nil
	t.Cleanup(func() { models.DB = database })
	first, err := PrepareDurableGraphIntent(request)
	require.NoError(t, err)
	second, err := PrepareDurableGraphIntent(request)
	require.NoError(t, err)
	require.Equal(t, first, second)
	for _, job := range first.Jobs {
		var spec scheduler.JobJson
		require.NoError(t, json.Unmarshal(job.SerializedSpec, &spec))
		require.Empty(t, spec.BackendJobToken)
		require.Empty(t, spec.BackendHostname)
		require.Empty(t, spec.BackendOrganisationName)
	}
}

func TestFrozenGraphBuilderRejectsCyclesAndUnknownSpecFields(t *testing.T) {
	_, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	intent, err := PrepareDurableGraphIntent(request)
	require.NoError(t, err)
	batchID, err := operation.DeriveBatch(operation.ID(request.Identity.DeliveryOperationID), string(request.JobType), request.RepoFullName, request.PullRequestNumber, request.CommitSHA)
	require.NoError(t, err)
	for _, mutate := range []func(*models.DurableGraphIntent){
		func(frozen *models.DurableGraphIntent) {
			frozen.Jobs[0].Parents = []string{frozen.Jobs[1].ProjectName}
			frozen.Jobs[1].Parents = []string{frozen.Jobs[0].ProjectName}
		},
		func(frozen *models.DurableGraphIntent) {
			var spec map[string]any
			require.NoError(t, json.Unmarshal(frozen.Jobs[0].SerializedSpec, &spec))
			spec["unknown_execution_field"] = true
			frozen.Jobs[0].SerializedSpec, err = json.Marshal(spec)
			require.NoError(t, err)
		},
		func(frozen *models.DurableGraphIntent) {
			frozen.Jobs[0].SerializedSpec = append(frozen.Jobs[0].SerializedSpec, []byte(` {}`)...)
		},
		func(frozen *models.DurableGraphIntent) {
			frozen.JobType = "unsupported"
			for index := range frozen.Jobs {
				var spec scheduler.JobJson
				require.NoError(t, json.Unmarshal(frozen.Jobs[index].SerializedSpec, &spec))
				spec.JobType = "unsupported"
				frozen.Jobs[index].SerializedSpec, err = json.Marshal(spec)
				require.NoError(t, err)
			}
		},
	} {
		frozen, err := cloneDurableGraphIntent(*intent)
		require.NoError(t, err)
		mutate(frozen)
		_, _, _, err = prepareFrozenDurableJobs(*frozen, batchID)
		require.Error(t, err)
	}
}

func TestPrepareDurableGraphRejectsUnsupportedTypeAndUnselectedCheckMetadata(t *testing.T) {
	_, organisation, delivery := newDurableGraphTestDatabase(t)
	request := durableGraphTestRequest(t, organisation, delivery)
	request.JobCheckRunDataByProject = map[string]CheckRunData{"not-selected": {Id: "123", Url: "https://github.com/checks/123"}}
	_, err := PrepareDurableGraphIntent(request)
	require.ErrorContains(t, err, "unselected project")
	request.JobCheckRunDataByProject = nil
	request.JobType = "unsupported"
	_, err = PrepareDurableGraphIntent(request)
	require.ErrorContains(t, err, "unknown durable job type")
}
