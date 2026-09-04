-- OIDC claims are required before dormant execution grants can be enabled.
-- Existing claims cannot be retroactively attested by GitHub.
SET LOCAL lock_timeout = '5s';

DO $migration$
BEGIN
  IF EXISTS (SELECT 1 FROM "public"."execution_claim_attempts") THEN
    RAISE EXCEPTION 'execution claims must be empty before OIDC identity binding';
  END IF;
END
$migration$;

ALTER TABLE "public"."execution_claim_attempts"
  ADD COLUMN IF NOT EXISTS "repository_id" bigint NOT NULL,
  ADD COLUMN IF NOT EXISTS "oidc_issuer" text NOT NULL,
  ADD COLUMN IF NOT EXISTS "oidc_audience" text NOT NULL,
  ADD COLUMN IF NOT EXISTS "oidc_subject" text NOT NULL;

DO $migration$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_oidc_identity_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    ALTER TABLE "public"."execution_claim_attempts"
      ADD CONSTRAINT "execution_claim_attempts_oidc_identity_check"
      CHECK (repository_id > 0 AND length(oidc_issuer) BETWEEN 1 AND 256 AND length(oidc_audience) BETWEEN 1 AND 1024 AND length(oidc_subject) BETWEEN 1 AND 1024);
  END IF;
END
$migration$;
