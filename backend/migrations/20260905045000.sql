SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS "public"."execution_grant_keys" (
  "key_id" text NOT NULL,
  "secret_sha256" text NOT NULL,
  "registered_at" timestamptz NOT NULL,
  PRIMARY KEY ("key_id"),
  CONSTRAINT "execution_grant_keys_key_id_check"
    CHECK (length(key_id) BETWEEN 1 AND 128 AND trim(key_id) = key_id),
  CONSTRAINT "execution_grant_keys_fingerprint_check"
    CHECK (length(secret_sha256) = 64 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(secret_sha256,'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'a',''),'b',''),'c',''),'d',''),'e',''),'f','')) = 0)
);
