-- atlas:txmode none

-- Allow one webhook delivery to own a batch and multiple job operations. Build
-- the replacement before removing the unique index so retries retain an index.
DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_control_operations_delivery_lookup";
CREATE INDEX CONCURRENTLY "idx_control_operations_delivery_lookup"
  ON "public"."control_operations" ("delivery_id") WHERE "delivery_id" IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_control_operations_delivery_id";

-- Bind protocol-v2 batches and jobs to durable control operations. Existing
-- protocol-v1 rows remain nullable and unchanged. Each lock-taking statement
-- sets its own transaction-local timeout so an Atlas partial retry remains
-- fail-fast even when it resumes in a new database session.
DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."digger_batches"
      ADD CONSTRAINT "fk_digger_batches_operation"
      FOREIGN KEY ("operation_id") REFERENCES "public"."control_operations" ("operation_id")
      ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."digger_batches"
      VALIDATE CONSTRAINT "fk_digger_batches_operation"
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."digger_jobs"
      ADD CONSTRAINT "fk_digger_jobs_operation"
      FOREIGN KEY ("operation_id") REFERENCES "public"."control_operations" ("operation_id")
      ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."digger_jobs"
      VALIDATE CONSTRAINT "fk_digger_jobs_operation"
  $ddl$;
END
$migration$;

-- Protocol-v2 job tokens are bound to one database job row. Existing tokens
-- remain organization-scoped for the legacy protocol until it is retired.
DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."job_tokens"
      ADD COLUMN "digger_job_database_id" bigint NULL,
      ADD CONSTRAINT "fk_job_tokens_digger_job"
      FOREIGN KEY ("digger_job_database_id") REFERENCES "public"."digger_jobs" ("id")
      ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."job_tokens"
      VALIDATE CONSTRAINT "fk_job_tokens_digger_job"
  $ddl$;
END
$migration$;

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_job_tokens_digger_job_database_id";
CREATE UNIQUE INDEX CONCURRENTLY "idx_job_tokens_digger_job_database_id"
  ON "public"."job_tokens" ("digger_job_database_id")
  WHERE "digger_job_database_id" IS NOT NULL;
