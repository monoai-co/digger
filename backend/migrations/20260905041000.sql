-- Add the immutable dispatch epoch separately from the writer epoch that
-- grants an execution claim. In-flight epoch N workflows may be admitted by
-- the authoritative epoch N+1 control plane during a switchover.
DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."execution_claim_attempts"
      ADD COLUMN IF NOT EXISTS "dispatch_writer_epoch" bigint NULL
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "public"."execution_claim_attempts"
    WHERE "dispatch_writer_epoch" IS NULL
  ) THEN
    RAISE EXCEPTION 'execution claim attempts require a dispatch writer epoch';
  END IF;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."execution_claim_attempts"
      ALTER COLUMN "dispatch_writer_epoch" SET NOT NULL
  $ddl$;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'execution_claim_attempts_epochs_check'
      AND conrelid = 'public.execution_claim_attempts'::regclass
  ) THEN
    EXECUTE $ddl$
      ALTER TABLE "public"."execution_claim_attempts"
        ADD CONSTRAINT "execution_claim_attempts_epochs_check"
        CHECK ("dispatch_writer_epoch" > 0 AND "expected_writer_epoch" > 0) NOT VALID
    $ddl$;
  END IF;
END
$migration$;

DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    ALTER TABLE "public"."execution_claim_attempts"
      VALIDATE CONSTRAINT "execution_claim_attempts_epochs_check"
  $ddl$;
END
$migration$;
