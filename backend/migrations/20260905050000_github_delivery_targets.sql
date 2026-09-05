-- Persist the first selected PR/head independently of execution graph creation.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS "public"."github_delivery_targets" (
  "delivery_operation_id" text NOT NULL,
  "organisation_id" bigint NOT NULL,
  "target" jsonb NOT NULL,
  "target_sha256" text NOT NULL,
  "delivery_payload_sha256" text NOT NULL,
  "writer_epoch" bigint NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("delivery_operation_id"),
  CONSTRAINT "fk_github_delivery_targets_delivery_operation" FOREIGN KEY ("delivery_operation_id") REFERENCES "public"."control_operations" ("operation_id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_github_delivery_targets_organisation" FOREIGN KEY ("organisation_id") REFERENCES "public"."organisations" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "github_delivery_target_digest_check" CHECK (length(target_sha256) = 64)
);

CREATE OR REPLACE FUNCTION public.guard_github_delivery_target_history()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'GitHub delivery target history is immutable' USING ERRCODE = '23514';
END
$function$;

DO $migration$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_delivery_target_history_rows' AND tgrelid = 'public.github_delivery_targets'::regclass) THEN
    CREATE TRIGGER github_delivery_target_history_rows BEFORE UPDATE OR DELETE
      ON public.github_delivery_targets FOR EACH ROW EXECUTE FUNCTION public.guard_github_delivery_target_history();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_delivery_target_history_truncate' AND tgrelid = 'public.github_delivery_targets'::regclass) THEN
    CREATE TRIGGER github_delivery_target_history_truncate BEFORE TRUNCATE
      ON public.github_delivery_targets FOR EACH STATEMENT EXECUTE FUNCTION public.guard_github_delivery_target_history();
  END IF;
END
$migration$;
