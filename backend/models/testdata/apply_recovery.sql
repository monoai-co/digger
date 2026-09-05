-- Run on a disposable migrated PostgreSQL database. Clone the row shape to
-- exercise the production trigger without constructing an execution graph.
BEGIN;
CREATE TEMP TABLE recovery_history_test (LIKE public.apply_recoveries INCLUDING ALL);
CREATE TRIGGER history_rows BEFORE INSERT OR UPDATE OR DELETE ON recovery_history_test
  FOR EACH ROW EXECUTE FUNCTION public.guard_apply_recovery_history();
CREATE TRIGGER history_truncate BEFORE TRUNCATE ON recovery_history_test
  FOR EACH STATEMENT EXECUTE FUNCTION public.guard_apply_recovery_history();

DO $test$
DECLARE
  mutation text;
BEGIN
  IF (SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.apply_recoveries'::regclass
      AND contype = 'f' AND confupdtype = 'r' AND confdeltype = 'r') <> 3 THEN
    RAISE EXCEPTION 'recovery identity foreign keys are missing';
  END IF;
  IF (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.apply_recoveries'::regclass
      AND tgname IN ('apply_recovery_history_rows', 'apply_recovery_history_truncate')
      AND tgenabled = 'O' AND tgfoid = 'public.guard_apply_recovery_history()'::regprocedure) <> 2 THEN
    RAISE EXCEPTION 'production recovery history triggers are missing';
  END IF;
END
$test$;

INSERT INTO recovery_history_test
  (operation_id, organisation_id, execution_claim_id, writer_epoch, observation, observation_sha256, created_at)
VALUES ('recovery-test', 1, '00000000-0000-4000-8000-000000000001', 7,
        '{"status":"in_progress"}', repeat('a',64), clock_timestamp());

DO $test$
DECLARE mutation text;
BEGIN
  FOREACH mutation IN ARRAY ARRAY[
    'UPDATE recovery_history_test SET operation_id = ''changed''',
    'UPDATE recovery_history_test SET organisation_id = 2',
    'UPDATE recovery_history_test SET execution_claim_id = ''00000000-0000-4000-8000-000000000002''',
    'UPDATE recovery_history_test SET writer_epoch = 8',
    'UPDATE recovery_history_test SET observation = ''{}''',
    'UPDATE recovery_history_test SET observation_sha256 = repeat(''b'',64)',
    'UPDATE recovery_history_test SET created_at = created_at + interval ''1 second''',
    'UPDATE recovery_history_test SET revision = 2',
    'UPDATE recovery_history_test SET outcome = ''aborted''',
    'UPDATE recovery_history_test SET terminal_observation = ''{"status":"in_progress"}'', terminal_observation_sha256 = repeat(''b'',64)',
    'UPDATE recovery_history_test SET terminal_observation_sha256 = repeat(''b'',64)',
    'DELETE FROM recovery_history_test',
    'TRUNCATE recovery_history_test'
  ] LOOP
    BEGIN
      EXECUTE mutation;
      RAISE EXCEPTION 'history mutation unexpectedly succeeded: %', mutation;
    EXCEPTION WHEN check_violation THEN NULL;
    END;
  END LOOP;
END
$test$;

UPDATE recovery_history_test SET terminal_observation = '{"status":"completed"}', terminal_observation_sha256 = repeat('b',64);
DO $test$
BEGIN
  BEGIN
    UPDATE recovery_history_test SET terminal_observation = '{"status":"completed","conclusion":"success"}';
    RAISE EXCEPTION 'terminal evidence replacement unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    UPDATE recovery_history_test SET terminal_observation = NULL, terminal_observation_sha256 = '';
    RAISE EXCEPTION 'terminal evidence removal unexpectedly succeeded';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
END
$test$;

UPDATE recovery_history_test SET outcome = 'aborted', revision = 2,
  resolution_id = '00000000-0000-4000-8000-000000000003', resolution_sha256 = repeat('c',64),
  resolution = '{"actor":"regression-test"}', resolved_at = clock_timestamp();

DO $test$
DECLARE mutation text;
BEGIN
  FOREACH mutation IN ARRAY ARRAY[
    'UPDATE recovery_history_test SET outcome = ''verified_succeeded''',
    'UPDATE recovery_history_test SET resolution = ''{}''',
    'UPDATE recovery_history_test SET revision = revision',
    'DELETE FROM recovery_history_test',
    'TRUNCATE recovery_history_test'
  ] LOOP
    BEGIN
      EXECUTE mutation;
      RAISE EXCEPTION 'resolved history mutation unexpectedly succeeded: %', mutation;
    EXCEPTION WHEN check_violation THEN NULL;
    END;
  END LOOP;
  IF NOT EXISTS (SELECT 1 FROM recovery_history_test WHERE outcome = 'aborted' AND revision = 2
      AND observation_sha256 = repeat('a',64) AND terminal_observation_sha256 = repeat('b',64)) THEN
    RAISE EXCEPTION 'recovery evidence was not preserved';
  END IF;
END
$test$;
ROLLBACK;
