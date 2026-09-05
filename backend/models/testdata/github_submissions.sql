BEGIN;
CREATE TEMP TABLE submission_history_test (LIKE public.github_submissions INCLUDING ALL);
CREATE TRIGGER history_rows BEFORE UPDATE OR DELETE ON submission_history_test
  FOR EACH ROW EXECUTE FUNCTION public.guard_github_submission_history();
CREATE TRIGGER history_truncate BEFORE TRUNCATE ON submission_history_test
  FOR EACH STATEMENT EXECUTE FUNCTION public.guard_github_submission_history();

INSERT INTO submission_history_test
  (delivery_operation_id, organisation_id, intent, intent_sha256, delivery_payload_sha256, writer_epoch, created_at)
VALUES ('submission-test', 1, '{"graph":{},"sources":[]}', repeat('a',64), repeat('b',64), 7, clock_timestamp());

DO $test$
DECLARE mutation text;
BEGIN
  FOREACH mutation IN ARRAY ARRAY[
    'UPDATE submission_history_test SET delivery_operation_id = ''changed''',
    'UPDATE submission_history_test SET organisation_id = 2',
    'UPDATE submission_history_test SET intent = ''{}''',
    'UPDATE submission_history_test SET intent_sha256 = repeat(''c'',64)',
    'UPDATE submission_history_test SET delivery_payload_sha256 = repeat(''c'',64)',
    'UPDATE submission_history_test SET writer_epoch = 8',
    'UPDATE submission_history_test SET created_at = clock_timestamp()',
    'UPDATE submission_history_test SET intent = intent',
    'DELETE FROM submission_history_test',
    'TRUNCATE submission_history_test'
  ] LOOP
    BEGIN
      EXECUTE mutation;
      RAISE EXCEPTION 'submission mutation unexpectedly succeeded: %', mutation;
    EXCEPTION WHEN check_violation THEN NULL;
    END;
  END LOOP;
  IF (SELECT count(*) FROM submission_history_test) <> 1 THEN
    RAISE EXCEPTION 'submission history was lost';
  END IF;
  IF (SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.github_submissions'::regclass
      AND contype = 'f' AND confupdtype = 'r' AND confdeltype = 'r') <> 2 THEN
    RAISE EXCEPTION 'submission identity foreign keys are missing';
  END IF;
END
$test$;
ROLLBACK;
