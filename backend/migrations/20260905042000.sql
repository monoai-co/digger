-- atlas:txmode none

-- Execution claims are dormant until the cross-account control-plane cutover.
-- Refuse to infer exact job, token, or signing-key identities for any rows that
-- predate this contract.
DO $migration$
BEGIN
  IF EXISTS (SELECT 1 FROM "public"."execution_claim_attempts") THEN
    RAISE EXCEPTION 'execution claim attempts must be empty before exact claim binding';
  END IF;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."execution_claim_attempts"
      ADD COLUMN IF NOT EXISTS "digger_job_id" text NULL,
      ADD COLUMN IF NOT EXISTS "digger_job_database_id" bigint NULL,
      ADD COLUMN IF NOT EXISTS "job_token_id" bigint NULL,
      ADD COLUMN IF NOT EXISTS "claim_sha256" text NULL,
      ADD COLUMN IF NOT EXISTS "signing_key_id" text NULL,
      ADD COLUMN IF NOT EXISTS "grant_expires_at" timestamptz NULL
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."execution_claim_attempts"
      ALTER COLUMN "digger_job_id" SET NOT NULL,
      ALTER COLUMN "digger_job_database_id" SET NOT NULL,
      ALTER COLUMN "job_token_id" SET NOT NULL,
      ALTER COLUMN "claim_sha256" SET NOT NULL,
      ALTER COLUMN "signing_key_id" SET NOT NULL,
      ALTER COLUMN "grant_token_sha256" SET NOT NULL,
      ALTER COLUMN "grant_expires_at" SET NOT NULL
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "public"."digger_jobs"
    WHERE "operation_id" IS NOT NULL
    GROUP BY "operation_id", "digger_job_id"
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'duplicate durable operation and job identities must be repaired before migration';
  END IF;
END
$migration$;

-- An interrupted CREATE INDEX CONCURRENTLY leaves an invalid same-named
-- index. Remove only those unusable remnants so a normal migration retry can
-- recreate them; never drop a valid index that may already back a constraint.
DO $migration$
DECLARE
  index_name text;
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  FOREACH index_name IN ARRAY ARRAY[
    'idx_digger_jobs_operation_public_id',
    'idx_digger_jobs_exact_identity',
    'idx_job_tokens_job_token_identity'
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

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_digger_jobs_operation_public_id"
  ON "public"."digger_jobs" ("operation_id", "digger_job_id");

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_digger_jobs_exact_identity"
  ON "public"."digger_jobs" ("id", "operation_id", "digger_job_id");

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_job_tokens_job_token_identity"
  ON "public"."job_tokens" ("digger_job_database_id", "id");

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_execution_claim_attempts_exact_job'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "fk_execution_claim_attempts_exact_job"
        FOREIGN KEY ("digger_job_database_id", "operation_id", "digger_job_id")
        REFERENCES "public"."digger_jobs" ("id", "operation_id", "digger_job_id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_execution_claim_attempts_exact_job_token'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "fk_execution_claim_attempts_exact_job_token"
        FOREIGN KEY ("digger_job_database_id", "job_token_id")
        REFERENCES "public"."job_tokens" ("digger_job_database_id", "id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."execution_claim_attempts"
      VALIDATE CONSTRAINT "fk_execution_claim_attempts_exact_job"
  $ddl$;
  EXECUTE $ddl$
    ALTER TABLE "public"."execution_claim_attempts"
      VALIDATE CONSTRAINT "fk_execution_claim_attempts_exact_job_token"
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_grant_expiry_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "execution_claim_attempts_grant_expiry_check"
        CHECK (
          "grant_expires_at" > "created_at" AND
          ("granted_at" IS NULL OR "grant_expires_at" > "granted_at")
        ) NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_positive_ids_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "execution_claim_attempts_positive_ids_check"
        CHECK (
          "digger_job_database_id" > 0 AND "job_token_id" > 0 AND
          "run_id" > 0 AND "run_attempt" > 0 AND "protocol_version" > 0
        ) NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_claim_digest_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "execution_claim_attempts_claim_digest_check"
        CHECK (
          length("claim_sha256") = 64 AND
          length(replace(replace(replace(replace(replace(replace(replace(replace(
            replace(replace(replace(replace(replace(replace(replace(replace(
              "claim_sha256", '0', ''), '1', ''), '2', ''), '3', ''),
              '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''),
              'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0
        ) NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_grant_digest_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "execution_claim_attempts_grant_digest_check"
        CHECK (
          "grant_token_sha256" = '' OR (
            length("grant_token_sha256") = 64 AND
            length(replace(replace(replace(replace(replace(replace(replace(replace(
              replace(replace(replace(replace(replace(replace(replace(replace(
                "grant_token_sha256", '0', ''), '1', ''), '2', ''), '3', ''),
                '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''),
                'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '')) = 0
          )
        ) NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_signing_key_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "execution_claim_attempts_signing_key_check"
        CHECK (
          length("signing_key_id") BETWEEN 1 AND 128 AND
          trim("signing_key_id") = "signing_key_id"
        ) NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_lifecycle_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "execution_claim_attempts_lifecycle_check"
        CHECK (
          ("state" = 'granted' AND "grant_token_sha256" <> '' AND "granted_at" IS NOT NULL AND "rejected_at" IS NULL) OR
          ("state" = 'rejected' AND "grant_token_sha256" = '' AND "granted_at" IS NULL AND "rejected_at" IS NOT NULL) OR
          ("state" = 'pending' AND "grant_token_sha256" = '' AND "granted_at" IS NULL AND "rejected_at" IS NULL)
        ) NOT VALID
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
    'execution_claim_attempts_positive_ids_check',
    'execution_claim_attempts_claim_digest_check',
    'execution_claim_attempts_grant_digest_check',
    'execution_claim_attempts_signing_key_check',
    'execution_claim_attempts_grant_expiry_check',
    'execution_claim_attempts_lifecycle_check'
  ]
  LOOP
    EXECUTE format(
      'ALTER TABLE "public"."execution_claim_attempts" VALIDATE CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
END
$migration$;

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_execution_claim_grant_digest";
CREATE UNIQUE INDEX CONCURRENTLY "idx_execution_claim_grant_digest"
  ON "public"."execution_claim_attempts" ("grant_token_sha256")
  WHERE "state" = 'granted';
