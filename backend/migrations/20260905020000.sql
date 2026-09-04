-- atlas:txmode none

-- Build indexes on existing high-write tables without blocking inserts or updates.
CREATE UNIQUE INDEX CONCURRENTLY "idx_digger_batches_operation_id"
  ON "public"."digger_batches" ("operation_id") WHERE "operation_id" IS NOT NULL;

CREATE UNIQUE INDEX CONCURRENTLY "idx_digger_jobs_operation_id"
  ON "public"."digger_jobs" ("operation_id") WHERE "operation_id" IS NOT NULL;
