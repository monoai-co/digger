SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS "public"."github_report_create_attempts" (
  "effect_id" uuid NOT NULL,
  "operation_id" text NOT NULL,
  "effect_key" text NOT NULL,
  "payload_sha256" text NOT NULL,
  "writer_epoch" bigint NOT NULL,
  "lease_id" text NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("effect_id"),
  CONSTRAINT "fk_github_report_create_attempts_effect" FOREIGN KEY ("effect_id") REFERENCES "public"."outbox_effects" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "github_report_create_attempt_digest_check" CHECK (length(payload_sha256) = 64)
);

CREATE TABLE IF NOT EXISTS "public"."github_report_receipts" (
  "effect_id" uuid NOT NULL,
  "payload_sha256" text NOT NULL,
  "resource_kind" text NOT NULL,
  "provider_id" bigint NOT NULL,
  "provider_url" text NOT NULL,
  "provider_identity_sha256" text NOT NULL,
  "created_at" timestamptz NOT NULL,
  PRIMARY KEY ("effect_id"),
  CONSTRAINT "fk_github_report_receipts_attempt" FOREIGN KEY ("effect_id") REFERENCES "public"."github_report_create_attempts" ("effect_id") ON UPDATE RESTRICT ON DELETE RESTRICT,
  CONSTRAINT "github_report_receipt_digest_check" CHECK (length(payload_sha256) = 64),
  CONSTRAINT "github_report_receipt_provider_identity_check" CHECK (length(provider_identity_sha256) = 64)
);

CREATE UNIQUE INDEX IF NOT EXISTS "idx_github_report_receipt_provider_identity"
  ON "public"."github_report_receipts" ("provider_identity_sha256");

CREATE OR REPLACE FUNCTION public.guard_outbox_effect_identity()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  IF ROW(NEW.id, NEW.operation_id, NEW.effect_kind, NEW.effect_key, NEW.payload, NEW.payload_sha256)
      IS DISTINCT FROM ROW(OLD.id, OLD.operation_id, OLD.effect_kind, OLD.effect_key, OLD.payload, OLD.payload_sha256) THEN
    RAISE EXCEPTION 'Outbox effect identity is immutable' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION public.guard_github_report_history()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'GitHub report history is immutable' USING ERRCODE = '23514';
END
$function$;

DO $migration$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'outbox_effect_identity_rows' AND tgrelid = 'public.outbox_effects'::regclass) THEN
    CREATE TRIGGER outbox_effect_identity_rows BEFORE UPDATE ON public.outbox_effects
      FOR EACH ROW EXECUTE FUNCTION public.guard_outbox_effect_identity();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_report_attempt_history_rows' AND tgrelid = 'public.github_report_create_attempts'::regclass) THEN
    CREATE TRIGGER github_report_attempt_history_rows BEFORE UPDATE OR DELETE
      ON public.github_report_create_attempts FOR EACH ROW EXECUTE FUNCTION public.guard_github_report_history();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_report_attempt_history_truncate' AND tgrelid = 'public.github_report_create_attempts'::regclass) THEN
    CREATE TRIGGER github_report_attempt_history_truncate BEFORE TRUNCATE
      ON public.github_report_create_attempts FOR EACH STATEMENT EXECUTE FUNCTION public.guard_github_report_history();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_report_receipt_history_rows' AND tgrelid = 'public.github_report_receipts'::regclass) THEN
    CREATE TRIGGER github_report_receipt_history_rows BEFORE UPDATE OR DELETE
      ON public.github_report_receipts FOR EACH ROW EXECUTE FUNCTION public.guard_github_report_history();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'github_report_receipt_history_truncate' AND tgrelid = 'public.github_report_receipts'::regclass) THEN
    CREATE TRIGGER github_report_receipt_history_truncate BEFORE TRUNCATE
      ON public.github_report_receipts FOR EACH STATEMENT EXECUTE FUNCTION public.guard_github_report_history();
  END IF;
END
$migration$;
