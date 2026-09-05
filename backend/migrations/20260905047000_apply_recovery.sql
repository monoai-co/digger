-- New recovery history is additive; all DDL rolls back together on lock timeout.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS "public"."apply_recoveries" (
  "operation_id" text NOT NULL,
  "organisation_id" bigint NOT NULL,
  "execution_claim_id" uuid NOT NULL,
  "writer_epoch" bigint NOT NULL,
  "revision" bigint NOT NULL DEFAULT 1,
  "outcome" text NOT NULL DEFAULT 'unknown',
  "observation" jsonb NOT NULL,
  "observation_sha256" text NOT NULL,
  "terminal_observation" jsonb NULL,
  "terminal_observation_sha256" text NOT NULL DEFAULT '',
  "created_at" timestamptz NOT NULL,
  "resolution_id" uuid NULL,
  "resolution_sha256" text NOT NULL DEFAULT '',
  "resolution" jsonb NULL,
  "resolved_at" timestamptz NULL,
  PRIMARY KEY ("operation_id"),
  CONSTRAINT "fk_apply_recoveries_claim" FOREIGN KEY ("execution_claim_id") REFERENCES "public"."execution_claim_attempts" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_apply_recoveries_operation" FOREIGN KEY ("operation_id") REFERENCES "public"."control_operations" ("operation_id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_apply_recoveries_organisation" FOREIGN KEY ("organisation_id") REFERENCES "public"."organisations" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "apply_recovery_outcome_check" CHECK (outcome IN ('unknown', 'verified_succeeded', 'aborted'))
);

CREATE UNIQUE INDEX IF NOT EXISTS "idx_apply_recovery_claim" ON "public"."apply_recoveries" ("execution_claim_id");
CREATE INDEX IF NOT EXISTS "idx_apply_recovery_org" ON "public"."apply_recoveries" ("organisation_id");
CREATE UNIQUE INDEX IF NOT EXISTS "idx_apply_recovery_resolution" ON "public"."apply_recoveries" ("resolution_id");

CREATE OR REPLACE FUNCTION public.guard_apply_recovery_history()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    RAISE EXCEPTION 'apply recovery history cannot be removed' USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'INSERT' AND NEW.outcome <> 'unknown' THEN
    RAISE EXCEPTION 'apply recovery must begin unresolved' USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'UPDATE' THEN
    IF OLD.outcome <> 'unknown' THEN
      RAISE EXCEPTION 'resolved apply recovery is immutable' USING ERRCODE = '23514';
    END IF;
    IF ROW(NEW.operation_id, NEW.organisation_id, NEW.execution_claim_id, NEW.writer_epoch,
           NEW.observation, NEW.observation_sha256, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.operation_id, OLD.organisation_id, OLD.execution_claim_id, OLD.writer_epoch,
           OLD.observation, OLD.observation_sha256, OLD.created_at) THEN
      RAISE EXCEPTION 'apply recovery identity and observation are immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.terminal_observation IS NOT NULL AND
       ROW(NEW.terminal_observation, NEW.terminal_observation_sha256) IS DISTINCT FROM
       ROW(OLD.terminal_observation, OLD.terminal_observation_sha256) THEN
      RAISE EXCEPTION 'terminal apply observation is immutable' USING ERRCODE = '23514';
    END IF;
  END IF;
  IF NEW.observation_sha256 !~ '^[0-9a-f]{64}$' OR
     (NEW.terminal_observation IS NULL AND NEW.terminal_observation_sha256 <> '') OR
     (NEW.terminal_observation IS NOT NULL AND
      (NEW.terminal_observation_sha256 !~ '^[0-9a-f]{64}$' OR
       COALESCE(NEW.terminal_observation->>'status', '') NOT IN ('completed', 'unavailable'))) THEN
    RAISE EXCEPTION 'apply recovery evidence is incomplete' USING ERRCODE = '23514';
  END IF;
  IF NEW.outcome = 'unknown' THEN
    IF NEW.revision <> 1 OR NEW.resolution_id IS NOT NULL OR NEW.resolution IS NOT NULL OR
       NEW.resolution_sha256 <> '' OR NEW.resolved_at IS NOT NULL THEN
      RAISE EXCEPTION 'unresolved apply recovery cannot contain a resolution' USING ERRCODE = '23514';
    END IF;
  ELSE
    IF NEW.revision <> 2 OR NEW.resolution_id IS NULL OR NEW.resolution IS NULL OR
       NEW.resolution_sha256 !~ '^[0-9a-f]{64}$' OR NEW.resolved_at IS NULL OR
       (NEW.terminal_observation IS NULL AND
        COALESCE(NEW.resolution #>> '{request,provider_unavailable}', '') <> 'true') THEN
      RAISE EXCEPTION 'apply recovery resolution is incomplete' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END
$function$;

DO $migration$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'apply_recovery_history_rows' AND tgrelid = 'public.apply_recoveries'::regclass) THEN
    CREATE TRIGGER apply_recovery_history_rows BEFORE INSERT OR UPDATE OR DELETE
      ON public.apply_recoveries FOR EACH ROW EXECUTE FUNCTION public.guard_apply_recovery_history();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'apply_recovery_history_truncate' AND tgrelid = 'public.apply_recoveries'::regclass) THEN
    CREATE TRIGGER apply_recovery_history_truncate BEFORE TRUNCATE
      ON public.apply_recoveries FOR EACH STATEMENT EXECUTE FUNCTION public.guard_apply_recovery_history();
  END IF;
END
$migration$;
