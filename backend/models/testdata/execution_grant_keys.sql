-- Run against a disposable database after applying the migration chain.
BEGIN;

INSERT INTO public.execution_grant_keys (key_id, secret_sha256, registered_at)
VALUES ('immutability-regression', repeat('a', 64), clock_timestamp());

DO $test$
BEGIN
  BEGIN
    UPDATE public.execution_grant_keys SET secret_sha256 = repeat('b', 64)
    WHERE key_id = 'immutability-regression';
    RAISE EXCEPTION 'fingerprint replacement unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    DELETE FROM public.execution_grant_keys WHERE key_id = 'immutability-regression';
    RAISE EXCEPTION 'key deletion unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    TRUNCATE public.execution_grant_keys;
    RAISE EXCEPTION 'key truncation unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  IF NOT EXISTS (
    SELECT 1 FROM public.execution_grant_keys
    WHERE key_id = 'immutability-regression' AND secret_sha256 = repeat('a', 64)
  ) THEN
    RAISE EXCEPTION 'original key identity was not preserved';
  END IF;
END
$test$;

ROLLBACK;
