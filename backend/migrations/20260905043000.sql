-- atlas:txmode none

-- Durable status callbacks are dormant until the new callback endpoint is
-- deployed. Existing rows cannot be bound to exact jobs, tokens, or claims.
DO $migration$
BEGIN
  IF EXISTS (SELECT 1 FROM "public"."job_status_callbacks") THEN
    RAISE EXCEPTION 'job status callbacks must be empty before exact callback binding';
  END IF;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."job_status_callbacks"
      ADD COLUMN IF NOT EXISTS "digger_job_database_id" bigint NULL,
      ADD COLUMN IF NOT EXISTS "job_token_id" bigint NULL,
      ADD COLUMN IF NOT EXISTS "execution_claim_attempt_id" uuid NULL,
      ADD COLUMN IF NOT EXISTS "target_status" text NULL,
      ADD COLUMN IF NOT EXISTS "expected_status_version" bigint NULL,
      ADD COLUMN IF NOT EXISTS "applied" boolean NOT NULL DEFAULT false
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."job_status_callbacks"
      ALTER COLUMN "digger_job_database_id" SET NOT NULL,
      ALTER COLUMN "job_token_id" SET NOT NULL,
      ALTER COLUMN "execution_claim_attempt_id" SET NOT NULL,
      ALTER COLUMN "target_status" SET NOT NULL,
      ALTER COLUMN "expected_status_version" SET NOT NULL
  $ddl$;
END
$migration$;

-- A failed concurrent build leaves an invalid relation that IF NOT EXISTS
-- would otherwise mistake for a usable exact-identity index.
DO $migration$
DECLARE
  index_name text;
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  FOREACH index_name IN ARRAY ARRAY[
    'idx_execution_claim_exact_identity',
    'idx_job_status_callbacks_applied_version'
  ]
  LOOP
    IF EXISTS (
      SELECT 1
      FROM pg_class AS relation
      JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
      JOIN pg_index AS index_state ON index_state.indexrelid = relation.oid
      WHERE namespace.nspname = 'public'
        AND relation.relname = index_name
        AND NOT index_state.indisvalid
    ) THEN
      EXECUTE format('DROP INDEX "public".%I', index_name);
    END IF;
  END LOOP;
END
$migration$;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_execution_claim_exact_identity"
  ON "public"."execution_claim_attempts" ("id", "operation_id", "digger_job_database_id", "job_token_id");

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_job_status_callbacks_applied_version"
  ON "public"."job_status_callbacks" ("operation_id", "status_version")
  WHERE "applied" = true;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_job_status_callbacks_exact_job'
      AND conrelid = 'public.job_status_callbacks'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."job_status_callbacks"
        ADD CONSTRAINT "fk_job_status_callbacks_exact_job"
        FOREIGN KEY ("digger_job_database_id", "operation_id", "digger_job_id")
        REFERENCES "public"."digger_jobs" ("id", "operation_id", "digger_job_id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_job_status_callbacks_exact_job_token'
      AND conrelid = 'public.job_status_callbacks'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."job_status_callbacks"
        ADD CONSTRAINT "fk_job_status_callbacks_exact_job_token"
        FOREIGN KEY ("digger_job_database_id", "job_token_id")
        REFERENCES "public"."job_tokens" ("digger_job_database_id", "id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_job_status_callbacks_exact_execution_claim'
      AND conrelid = 'public.job_status_callbacks'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."job_status_callbacks"
        ADD CONSTRAINT "fk_job_status_callbacks_exact_execution_claim"
        FOREIGN KEY ("execution_claim_attempt_id", "operation_id", "digger_job_database_id", "job_token_id")
        REFERENCES "public"."execution_claim_attempts" ("id", "operation_id", "digger_job_database_id", "job_token_id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
END
$migration$;

DO $migration$
DECLARE
  constraint_name text;
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  FOREACH constraint_name IN ARRAY ARRAY[
    'fk_job_status_callbacks_exact_job',
    'fk_job_status_callbacks_exact_job_token',
    'fk_job_status_callbacks_exact_execution_claim'
  ]
  LOOP
    EXECUTE format(
      'ALTER TABLE "public"."job_status_callbacks" VALIDATE CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'job_status_callbacks_positive_ids_check'
      AND conrelid = 'public.job_status_callbacks'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."job_status_callbacks"
        ADD CONSTRAINT "job_status_callbacks_positive_ids_check"
        CHECK (
          "digger_job_database_id" > 0 AND "job_token_id" > 0 AND
          "expected_status_version" > 0 AND "status_version" > 0 AND
          "response_status" BETWEEN 200 AND 599
        ) NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'job_status_callbacks_payload_digest_check'
      AND conrelid = 'public.job_status_callbacks'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."job_status_callbacks"
        ADD CONSTRAINT "job_status_callbacks_payload_digest_check"
        CHECK (
          length("payload_sha256") = 64 AND
          length(replace(replace(replace(replace(replace(replace(replace(replace(
            replace(replace(replace(replace(replace(replace(replace(replace(
              "payload_sha256", '0', ''), '1', ''), '2', ''), '3', ''),
              '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''),
              'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0
        ) NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'job_status_callbacks_target_status_check'
      AND conrelid = 'public.job_status_callbacks'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."job_status_callbacks"
        ADD CONSTRAINT "job_status_callbacks_target_status_check"
        CHECK ("target_status" IN ('started', 'succeeded', 'failed')) NOT VALID
    $ddl$;
  END IF;
END
$migration$;

DO $migration$
DECLARE
  constraint_name text;
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  FOREACH constraint_name IN ARRAY ARRAY[
    'job_status_callbacks_positive_ids_check',
    'job_status_callbacks_payload_digest_check',
    'job_status_callbacks_target_status_check'
  ]
  LOOP
    EXECUTE format(
      'ALTER TABLE "public"."job_status_callbacks" VALIDATE CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
END
$migration$;
