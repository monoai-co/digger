BEGIN;
CREATE TEMP TABLE report_outbox_test (LIKE public.outbox_effects INCLUDING ALL);
CREATE TRIGGER identity_rows BEFORE UPDATE ON report_outbox_test
  FOR EACH ROW EXECUTE FUNCTION public.guard_outbox_effect_identity();
INSERT INTO report_outbox_test (id,operation_id,effect_kind,effect_key,payload,payload_sha256,writer_epoch,status,created_at,updated_at)
VALUES ('00000000-0000-0000-0000-000000000001','operation','github_report_create','summary','{}',repeat('a',64),7,'pending',clock_timestamp(),clock_timestamp());
CREATE TEMP TABLE report_attempt_test (LIKE public.github_report_create_attempts INCLUDING ALL);
CREATE TEMP TABLE report_receipt_test (LIKE public.github_report_receipts INCLUDING ALL);
CREATE TRIGGER history_rows BEFORE UPDATE OR DELETE ON report_attempt_test
  FOR EACH ROW EXECUTE FUNCTION public.guard_github_report_history();
CREATE TRIGGER history_truncate BEFORE TRUNCATE ON report_attempt_test
  FOR EACH STATEMENT EXECUTE FUNCTION public.guard_github_report_history();
CREATE TRIGGER history_rows BEFORE UPDATE OR DELETE ON report_receipt_test
  FOR EACH ROW EXECUTE FUNCTION public.guard_github_report_history();
CREATE TRIGGER history_truncate BEFORE TRUNCATE ON report_receipt_test
  FOR EACH STATEMENT EXECUTE FUNCTION public.guard_github_report_history();
INSERT INTO report_attempt_test VALUES ('00000000-0000-0000-0000-000000000001','report-operation','summary',repeat('a',64),7,'lease',clock_timestamp());
INSERT INTO report_receipt_test VALUES ('00000000-0000-0000-0000-000000000001', repeat('a',64),'comment',123,'https://github.com/owner/repo/pull/42#issuecomment-123',repeat('b',64),clock_timestamp());
DO $test$
DECLARE mutation text;
BEGIN
  FOREACH mutation IN ARRAY ARRAY[
    'UPDATE report_outbox_test SET id = ''00000000-0000-0000-0000-000000000002''',
    'UPDATE report_outbox_test SET operation_id = ''changed''',
    'UPDATE report_outbox_test SET effect_kind = ''unrelated''',
    'UPDATE report_outbox_test SET effect_key = ''changed''',
    'UPDATE report_outbox_test SET payload = ''{"changed":true}''',
    'UPDATE report_outbox_test SET payload_sha256 = repeat(''b'',64)',
    'UPDATE report_attempt_test SET payload_sha256 = repeat(''b'',64)',
    'UPDATE report_attempt_test SET operation_id = ''changed-operation''',
    'UPDATE report_attempt_test SET effect_key = ''changed-key''',
    'UPDATE report_attempt_test SET writer_epoch = 8',
    'UPDATE report_attempt_test SET lease_id = ''another''',
    'UPDATE report_attempt_test SET created_at = clock_timestamp()',
    'UPDATE report_attempt_test SET effect_id = effect_id',
    'DELETE FROM report_attempt_test',
    'TRUNCATE report_attempt_test',
    'UPDATE report_receipt_test SET payload_sha256 = repeat(''b'',64)',
    'UPDATE report_receipt_test SET resource_kind = ''check_run''',
    'UPDATE report_receipt_test SET provider_id = 456',
    'UPDATE report_receipt_test SET provider_url = ''https://example.com''',
    'UPDATE report_receipt_test SET provider_identity_sha256 = repeat(''c'',64)',
    'UPDATE report_receipt_test SET created_at = clock_timestamp()',
    'UPDATE report_receipt_test SET effect_id = effect_id',
    'DELETE FROM report_receipt_test',
    'TRUNCATE report_receipt_test'
  ] LOOP
    BEGIN
      EXECUTE mutation;
      RAISE EXCEPTION 'report history mutation unexpectedly succeeded: %', mutation;
    EXCEPTION WHEN check_violation THEN NULL;
    END;
  END LOOP;
  IF (SELECT count(*) FROM report_attempt_test) <> 1 OR (SELECT count(*) FROM report_receipt_test) <> 1 THEN
    RAISE EXCEPTION 'report history was lost';
  END IF;
  IF (SELECT count(*) FROM pg_constraint WHERE conrelid IN ('public.github_report_create_attempts'::regclass,'public.github_report_receipts'::regclass)
      AND contype = 'f' AND confupdtype = 'r' AND confdeltype = 'r') <> 2 THEN
    RAISE EXCEPTION 'report identity foreign keys are missing';
  END IF;
END
$test$;
UPDATE report_outbox_test SET status = 'processing', lease_id = 'lease', writer_epoch = 8,
  lease_expires_at = clock_timestamp() + interval '1 minute', attempt_count = 1;
UPDATE report_outbox_test SET status = 'succeeded', provider_receipt = '{"id":123}', lease_id = '', lease_expires_at = NULL;
ROLLBACK;
