-- Create a durable, immutable inbox for GitHub App webhook deliveries.
CREATE TABLE "public"."control_plane_fence" (
  "id" smallint NOT NULL,
  "database_identity" text NOT NULL,
  "writer_epoch" bigint NOT NULL,
  "mode" text NOT NULL,
  "protocol_floor" integer NOT NULL DEFAULT 1,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "control_plane_fence_singleton_check" CHECK ("id" = 1),
  CONSTRAINT "control_plane_fence_writer_epoch_check" CHECK ("writer_epoch" > 0),
  CONSTRAINT "control_plane_fence_mode_check" CHECK ("mode" IN ('normal', 'hold', 'drain', 'fenced')),
  CONSTRAINT "control_plane_fence_protocol_floor_check" CHECK ("protocol_floor" > 0)
);

INSERT INTO "public"."control_plane_fence"
  ("id", "database_identity", "writer_epoch", "mode", "protocol_floor", "updated_at")
VALUES
  (1, 'unconfigured', 1, 'hold', 1, CURRENT_TIMESTAMP);

CREATE TABLE "public"."github_webhook_ordering_domains" (
  "ordering_domain" text NOT NULL,
  "next_sequence" bigint NOT NULL DEFAULT 1,
  "last_terminal_sequence" bigint NOT NULL DEFAULT 0,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("ordering_domain"),
  CONSTRAINT "github_webhook_ordering_domains_sequence_check"
    CHECK ("next_sequence" > "last_terminal_sequence")
);

CREATE TABLE "public"."github_webhook_deliveries" (
  "delivery_id" text NOT NULL,
  "operation_id" text NOT NULL,
  "payload_sha256" text NOT NULL,
  "payload" bytea NOT NULL,
  "event_type" text NOT NULL,
  "github_app_id" bigint NOT NULL,
  "hook_id" text NULL,
  "hook_installation_target_type" text NULL,
  "installation_id" bigint NULL,
  "repository_full_name" text NULL,
  "ordering_domain" text NOT NULL,
  "ordering_sequence" bigint NOT NULL,
  "writer_epoch" bigint NULL,
  "received_at" timestamptz NOT NULL,
  "processing_status" text NOT NULL DEFAULT 'pending',
  "attempt_count" bigint NOT NULL DEFAULT 0,
  "lease_id" text NULL,
  "lease_expires_at" timestamptz NULL,
  "next_attempt_at" timestamptz NULL,
  "processed_at" timestamptz NULL,
  "dead_lettered_at" timestamptz NULL,
  "terminal_result" text NULL,
  "last_error" text NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("delivery_id"),
  CONSTRAINT "github_webhook_deliveries_status_check" CHECK ("processing_status" IN ('pending', 'processing', 'retrying', 'succeeded', 'ignored', 'dead_letter'))
);

CREATE UNIQUE INDEX "idx_github_webhook_delivery_order"
  ON "public"."github_webhook_deliveries" ("ordering_domain", "ordering_sequence");

CREATE UNIQUE INDEX "idx_github_webhook_deliveries_operation_id"
  ON "public"."github_webhook_deliveries" ("operation_id");

CREATE INDEX "idx_github_webhook_delivery_queue"
  ON "public"."github_webhook_deliveries" ("processing_status", "next_attempt_at", "lease_expires_at");

CREATE TABLE "public"."github_webhook_delivery_requeues" (
  "id" uuid NOT NULL,
  "delivery_id" text NOT NULL,
  "actor" text NOT NULL,
  "reason" text NOT NULL,
  "requeued_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_github_webhook_delivery_requeues_delivery"
    FOREIGN KEY ("delivery_id") REFERENCES "public"."github_webhook_deliveries" ("delivery_id") ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX "idx_github_webhook_delivery_requeues_delivery_id"
  ON "public"."github_webhook_delivery_requeues" ("delivery_id");

CREATE TABLE "public"."control_operations" (
  "operation_id" text NOT NULL,
  "operation_kind" text NOT NULL,
  "identity_sha256" text NOT NULL,
  "delivery_id" text NULL,
  "writer_epoch" bigint NOT NULL,
  "protocol_version" integer NOT NULL DEFAULT 1,
  "status" text NOT NULL DEFAULT 'pending',
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("operation_id"),
  CONSTRAINT "control_operations_status_check" CHECK ("status" IN ('pending', 'completed', 'failed')),
  CONSTRAINT "fk_control_operations_delivery"
    FOREIGN KEY ("delivery_id") REFERENCES "public"."github_webhook_deliveries" ("delivery_id") ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE UNIQUE INDEX "idx_control_operations_delivery_id"
  ON "public"."control_operations" ("delivery_id") WHERE "delivery_id" IS NOT NULL;

CREATE TABLE "public"."outbox_effects" (
  "id" uuid NOT NULL,
  "operation_id" text NOT NULL,
  "effect_kind" text NOT NULL,
  "effect_key" text NOT NULL,
  "payload" jsonb NOT NULL,
  "payload_sha256" text NOT NULL,
  "writer_epoch" bigint NOT NULL,
  "status" text NOT NULL DEFAULT 'pending',
  "attempt_count" bigint NOT NULL DEFAULT 0,
  "lease_id" text NULL,
  "lease_expires_at" timestamptz NULL,
  "next_attempt_at" timestamptz NULL,
  "provider_receipt" jsonb NULL,
  "last_error" text NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_outbox_effects_operation"
    FOREIGN KEY ("operation_id") REFERENCES "public"."control_operations" ("operation_id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "outbox_effects_status_check" CHECK ("status" IN ('pending', 'processing', 'succeeded', 'retrying', 'dead_letter'))
);

CREATE UNIQUE INDEX "idx_outbox_effect_identity"
  ON "public"."outbox_effects" ("operation_id", "effect_kind", "effect_key");

CREATE INDEX "idx_outbox_effect_queue"
  ON "public"."outbox_effects" ("status", "next_attempt_at", "lease_expires_at");

CREATE TABLE "public"."execution_claim_attempts" (
  "id" uuid NOT NULL,
  "operation_id" text NOT NULL,
  "run_id" bigint NOT NULL,
  "run_attempt" bigint NOT NULL,
  "workflow_ref" text NOT NULL,
  "workflow_sha" text NOT NULL,
  "action_ref" text NOT NULL,
  "cli_sha256" text NOT NULL,
  "protocol_version" integer NOT NULL,
  "expected_writer_epoch" bigint NOT NULL,
  "state" text NOT NULL DEFAULT 'pending',
  "grant_token_sha256" text NULL,
  "granted_at" timestamptz NULL,
  "rejected_at" timestamptz NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_execution_claim_attempts_operation"
    FOREIGN KEY ("operation_id") REFERENCES "public"."control_operations" ("operation_id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "execution_claim_attempts_state_check" CHECK ("state" IN ('pending', 'granted', 'rejected'))
);

CREATE UNIQUE INDEX "idx_execution_claimant"
  ON "public"."execution_claim_attempts" ("operation_id", "run_id", "run_attempt");

CREATE UNIQUE INDEX "idx_execution_claim_granted_operation"
  ON "public"."execution_claim_attempts" ("operation_id") WHERE "state" = 'granted';

CREATE TABLE "public"."job_status_callbacks" (
  "callback_id" uuid NOT NULL,
  "operation_id" text NOT NULL,
  "digger_job_id" text NOT NULL,
  "payload_sha256" text NOT NULL,
  "status_version" bigint NOT NULL,
  "response_status" integer NOT NULL,
  "response_body" jsonb NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("callback_id"),
  CONSTRAINT "fk_job_status_callbacks_operation"
    FOREIGN KEY ("operation_id") REFERENCES "public"."control_operations" ("operation_id") ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX "idx_job_status_callbacks_operation_id"
  ON "public"."job_status_callbacks" ("operation_id");

CREATE INDEX "idx_job_status_callbacks_digger_job_id"
  ON "public"."job_status_callbacks" ("digger_job_id");

ALTER TABLE "public"."digger_batches"
  ADD COLUMN "operation_id" text NULL,
  ADD COLUMN "protocol_version" integer NOT NULL DEFAULT 1,
  ADD COLUMN "status_version" bigint NOT NULL DEFAULT 0,
  ADD COLUMN "writer_epoch" bigint NULL;

ALTER TABLE "public"."digger_jobs"
  ADD COLUMN "operation_id" text NULL,
  ADD COLUMN "protocol_version" integer NOT NULL DEFAULT 1,
  ADD COLUMN "status_version" bigint NOT NULL DEFAULT 0,
  ADD COLUMN "writer_epoch" bigint NULL;
