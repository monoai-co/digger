-- atlas:txmode none

-- One active installation may authorize exactly one Digger organisation. The
-- application also fails closed on duplicate legacy rows, while this database
-- invariant closes the concurrent read-then-write race in link creation.
DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "public"."github_app_installation_links"
    WHERE "status" = 1 AND "deleted_at" IS NULL
    GROUP BY "github_installation_id"
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'duplicate active GitHub installation links must be repaired before migration';
  END IF;
END
$migration$;

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_github_installation_active_link";
CREATE UNIQUE INDEX CONCURRENTLY "idx_github_installation_active_link"
  ON "public"."github_app_installation_links" ("github_installation_id")
  WHERE "status" = 1 AND "deleted_at" IS NULL;

-- Durable job tokens are minted inactive with stable values and activated from
-- database time immediately before each workflow dispatch attempt. Legacy
-- organisation-scoped tokens retain their existing expiry-only behavior.
DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."job_tokens"
      ADD COLUMN IF NOT EXISTS "activated_at" timestamptz NULL,
      ADD COLUMN IF NOT EXISTS "revoked_at" timestamptz NULL
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."digger_jobs"
      ADD COLUMN IF NOT EXISTS "dependency_operation_ids" jsonb NOT NULL DEFAULT '[]'::jsonb
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "public"."job_tokens"
    WHERE "value" IS NOT NULL
    GROUP BY "value"
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'duplicate job token values must be repaired before migration';
  END IF;
END
$migration$;

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_job_tokens_value";
CREATE UNIQUE INDEX CONCURRENTLY "idx_job_tokens_value"
  ON "public"."job_tokens" ("value")
  WHERE "value" IS NOT NULL;

-- Public job IDs are the stable identifiers carried by workflows and graph
-- edges. Make collisions impossible before adding referential constraints.
DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "public"."digger_jobs"
    GROUP BY "digger_job_id"
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'duplicate Digger job IDs must be repaired before migration';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM "public"."github_digger_job_links" AS link
    LEFT JOIN "public"."digger_jobs" AS job ON job."digger_job_id" = link."digger_job_id"
    WHERE job."id" IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM "public"."digger_job_parent_links" AS link
    LEFT JOIN "public"."digger_jobs" AS child ON child."digger_job_id" = link."digger_job_id"
    LEFT JOIN "public"."digger_jobs" AS parent ON parent."digger_job_id" = link."parent_digger_job_id"
    WHERE child."id" IS NULL OR parent."id" IS NULL
  ) THEN
    RAISE EXCEPTION 'orphan Digger job links must be repaired before migration';
  END IF;
END
$migration$;

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_digger_jobs_public_id";
CREATE UNIQUE INDEX CONCURRENTLY "idx_digger_jobs_public_id"
  ON "public"."digger_jobs" ("digger_job_id");

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_github_digger_job_links_digger_job_id";
CREATE INDEX CONCURRENTLY "idx_github_digger_job_links_digger_job_id"
  ON "public"."github_digger_job_links" ("digger_job_id");
DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_digger_job_parent_links_child_id";
CREATE INDEX CONCURRENTLY "idx_digger_job_parent_links_child_id"
  ON "public"."digger_job_parent_links" ("digger_job_id");
DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_digger_job_parent_links_parent_id";
CREATE INDEX CONCURRENTLY "idx_digger_job_parent_links_parent_id"
  ON "public"."digger_job_parent_links" ("parent_digger_job_id");

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "public"."digger_job_parent_links"
    GROUP BY "digger_job_id", "parent_digger_job_id"
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'duplicate Digger job dependency edges must be repaired before migration';
  END IF;
END
$migration$;

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_digger_job_parent_links_edge";
CREATE UNIQUE INDEX CONCURRENTLY "idx_digger_job_parent_links_edge"
  ON "public"."digger_job_parent_links" ("digger_job_id", "parent_digger_job_id");

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "public"."outbox_effects"
    WHERE "effect_kind" = 'github_workflow_dispatch'
    GROUP BY "operation_id"
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'duplicate GitHub workflow dispatch effects must be repaired before migration';
  END IF;
END
$migration$;

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_outbox_workflow_dispatch_operation";
CREATE UNIQUE INDEX CONCURRENTLY "idx_outbox_workflow_dispatch_operation"
  ON "public"."outbox_effects" ("operation_id")
  WHERE "effect_kind" = 'github_workflow_dispatch';

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_github_digger_job_links_digger_job'
      AND conrelid = 'public.github_digger_job_links'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."github_digger_job_links"
        ADD CONSTRAINT "fk_github_digger_job_links_digger_job"
        FOREIGN KEY ("digger_job_id") REFERENCES "public"."digger_jobs" ("digger_job_id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."github_digger_job_links"
      VALIDATE CONSTRAINT "fk_github_digger_job_links_digger_job"
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_digger_job_parent_links_digger_job'
      AND conrelid = 'public.digger_job_parent_links'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."digger_job_parent_links"
        ADD CONSTRAINT "fk_digger_job_parent_links_digger_job"
        FOREIGN KEY ("digger_job_id") REFERENCES "public"."digger_jobs" ("digger_job_id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_digger_job_parent_links_parent_digger_job'
      AND conrelid = 'public.digger_job_parent_links'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."digger_job_parent_links"
        ADD CONSTRAINT "fk_digger_job_parent_links_parent_digger_job"
        FOREIGN KEY ("parent_digger_job_id") REFERENCES "public"."digger_jobs" ("digger_job_id")
        ON UPDATE RESTRICT ON DELETE RESTRICT NOT VALID
    $ddl$;
  END IF;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."digger_job_parent_links"
      VALIDATE CONSTRAINT "fk_digger_job_parent_links_digger_job"
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."digger_job_parent_links"
      VALIDATE CONSTRAINT "fk_digger_job_parent_links_parent_digger_job"
  $ddl$;
END
$migration$;

-- A webhook delivery may own many job operations, but it may produce only one
-- canonical batch graph. Build the invariant without blocking live writes.
DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_control_operations_delivery_batch";
CREATE UNIQUE INDEX CONCURRENTLY "idx_control_operations_delivery_batch"
  ON "public"."control_operations" ("delivery_id")
  WHERE "delivery_id" IS NOT NULL AND "operation_kind" = 'digger_batch';
