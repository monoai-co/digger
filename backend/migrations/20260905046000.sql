-- atlas:txmode none

-- Key IDs retain their original fingerprints permanently. Rotation adds a new
-- row; it must not reinterpret grants already signed by an existing key ID.
DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  EXECUTE $ddl$
    CREATE OR REPLACE FUNCTION public.reject_execution_grant_key_mutation()
    RETURNS trigger LANGUAGE plpgsql AS $function$
    BEGIN
      RAISE EXCEPTION 'execution grant key identities are append-only' USING ERRCODE = '23514';
    END
    $function$
  $ddl$;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'execution_grant_keys_append_only' AND tgrelid = 'public.execution_grant_keys'::regclass) THEN
    CREATE TRIGGER execution_grant_keys_append_only
      BEFORE UPDATE OR DELETE OR TRUNCATE ON public.execution_grant_keys
      FOR EACH STATEMENT EXECUTE FUNCTION public.reject_execution_grant_key_mutation();
  END IF;
END
$migration$;

-- Repair an interrupted concurrent index build before retrying it.
DO $migration$
BEGIN
  PERFORM set_config('lock_timeout', '5s', true);
  IF EXISTS (
    SELECT 1 FROM pg_index JOIN pg_class ON pg_class.oid = pg_index.indexrelid
    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    WHERE pg_namespace.nspname = 'public'
      AND pg_class.relname = 'idx_execution_claim_unexpired_keys' AND NOT pg_index.indisvalid
  ) THEN
    DROP INDEX public.idx_execution_claim_unexpired_keys;
  END IF;
END
$migration$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_execution_claim_unexpired_keys"
  ON "public"."execution_claim_attempts" ("grant_expires_at", "signing_key_id") WHERE state = 'granted';
