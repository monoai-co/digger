package models

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresDurableControlPlaneSchemaRequiresTablesAndColumns(t *testing.T) {
	for _, missing := range []string{"outbox_effects", "job_status_callbacks", "apply_recoveries", "apply_recovery_revision", "ordering_domains", "delivery_payload", "job_status_version", "claim_subject"} {
		t.Run(missing, func(t *testing.T) {
			database := newPostgresOutboxTestDatabase(t)
			require.NoError(t, database.GormDB.AutoMigrate(&JobStatusCallback{}, &ApplyRecovery{}, &DiggerJobParentLink{}, &ExecutionGrantKey{}))
			require.NoError(t, database.CheckDurableControlPlaneSchema(context.Background()))
			switch missing {
			case "outbox_effects":
				require.NoError(t, database.GormDB.Migrator().DropTable(&OutboxEffect{}))
			case "job_status_callbacks":
				require.NoError(t, database.GormDB.Migrator().DropTable(&JobStatusCallback{}))
			case "apply_recoveries":
				require.NoError(t, database.GormDB.Migrator().DropTable(&ApplyRecovery{}))
			case "apply_recovery_revision":
				require.NoError(t, database.GormDB.Migrator().DropColumn(&ApplyRecovery{}, "Revision"))
			case "ordering_domains":
				require.NoError(t, database.GormDB.Migrator().DropTable(&GithubWebhookOrderingDomain{}))
			case "delivery_payload":
				require.NoError(t, database.GormDB.Migrator().DropColumn(&GithubWebhookDelivery{}, "Payload"))
			case "job_status_version":
				require.NoError(t, database.GormDB.Migrator().DropColumn(&DiggerJob{}, "StatusVersion"))
			case "claim_subject":
				require.NoError(t, database.GormDB.Migrator().DropColumn(&ExecutionClaimAttempt{}, "OIDCSubject"))
			}
			require.Error(t, database.CheckDurableControlPlaneSchema(context.Background()))
		})
	}
}
