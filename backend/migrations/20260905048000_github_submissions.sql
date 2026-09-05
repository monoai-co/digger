-- Submission receipts are additive and do not change legacy dispatch behavior.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS "public"."github_submissions" (
  "delivery_operation_id" text NOT NULL,
  "organisation_id" bigint NOT NULL,
  "intent" jsonb NOT NULL,
  "intent_sha256" text NOT NULL,
  "delivery_payload_sha256" text NOT NULL,
  "writer_epoch" bigint NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("delivery_operation_id"),
  CONSTRAINT "fk_github_submissions_delivery_operation" FOREIGN KEY ("delivery_operation_id") REFERENCES "public"."control_operations" ("operation_id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "fk_github_submissions_organisation" FOREIGN KEY ("organisation_id") REFERENCES "public"."organisations" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "github_submission_intent_digest_check" CHECK (length(intent_sha256) = 64)
);

CREATE OR REPLACE FUNCTION public.guard_github_submission_history()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'GitHub submission history is immutable' USING ERRCODE = '23514';
END
$function$;

DO $migration$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_submission_history_rows' AND tgrelid = 'public.github_submissions'::regclass) THEN
    CREATE TRIGGER github_submission_history_rows BEFORE UPDATE OR DELETE
      ON public.github_submissions FOR EACH ROW EXECUTE FUNCTION public.guard_github_submission_history();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_submission_history_truncate' AND tgrelid = 'public.github_submissions'::regclass) THEN
    CREATE TRIGGER github_submission_history_truncate BEFORE TRUNCATE
      ON public.github_submissions FOR EACH STATEMENT EXECUTE FUNCTION public.guard_github_submission_history();
  END IF;
END
$migration$;
