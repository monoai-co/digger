# Recovering an unknown execution outcome

This API is available only when durable webhook processing, database identity, and
writer epoch are configured. It is dormant in the legacy runtime. Apply the
additive database migrations before enabling it.

An execution grant authorizes work; it does not prove its outcome. If callback
credentials expire without a committed terminal callback, reconciliation records
`unknown` instead of inferring success or failure from GitHub. Until resolution,
new execution grants across the same organisation are blocked. Already-issued
grants are not cancelled by this guard. The scope is deliberately organisation-wide
because repository/project names do not establish a canonical backend-state boundary.
New execution grants last at most five hours, independently of legacy job-token
retention. Workflow execution limits and the callback retry budget must fit inside
that window. The current SRE and Marketplace apply jobs have two-hour limits.

The record preserves the first observation and a write-once completion/unavailability
observation. An unavailable GitHub run is not evidence that its executor stopped.
Committed callback receipts remain replayable after expiry; new callbacks cannot
revive expired execution permission. No recovery action automatically reruns apply
or force-unlocks an OpenTofu backend.

## Prerequisites

- Use an authenticated administrator for the recovery's organisation. CLI job tokens,
  ordinary access tokens, and NOOP authentication cannot resolve recovery.
- Establish that the original executor and child processes cannot still mutate AWS.
  Grant expiry alone does not terminate a runner or revoke its AWS credentials.
- Inspect the exact backend lock owner, state lineage/serial/version, any retained
  local recovery state, and the actual affected resources. Do not overwrite a newer
  state or remove a lock while its owner may still run.
- Retain an immutable evidence package containing executor termination, state,
  resource, and execution-result evidence. Record the SHA-256 of each part. The API
  validates those references and the lifecycle; the administrator is responsible
  for verifying the evidence, not merely supplying arbitrary hashes.

If the evidence cannot establish an outcome or safe cessation, leave the operation
unknown. A clean plan by itself does not establish which execution caused current
state or whether all side effects completed.

## API

Read `GET /admin/apply-recoveries/:operationID`. A different organisation receives
the same 404 as an absent operation. The response contains the execution claim,
writer epoch, revision, evidence, and resolution, without job tokens or grants.

Resolve using `PUT /admin/apply-recoveries/:operationID/resolutions/:resolutionID`.
The strict JSON body is limited to 16 KiB and requires:

| Field | Value |
| --- | --- |
| `resolution_id` | UUID matching the URL; retain it for every retry |
| `expected_revision` | The unresolved revision, currently `1` |
| `outcome` | `verified_succeeded` or `aborted` |
| `reason` | Nonempty explanation, at most 2048 characters |
| `executor_stopped` | `true`, after verification |
| `provider_unavailable` | `true` if no usable provider completion receipt exists, explicitly acknowledging that executor termination is established by independent evidence; otherwise `false` |
| `evidence_uri` | Immutable HTTPS or S3 package location, without URL credentials or query string |
| `executor_evidence_sha256` | Lowercase SHA-256 of executor-termination evidence |
| `state_evidence_sha256` | Lowercase SHA-256 of backend identity and state evidence |
| `resource_evidence_sha256` | Lowercase SHA-256 of actual-resource verification |
| `result_evidence_sha256` | Lowercase SHA-256 supporting the selected result |

`verified_succeeded` is an explicit operator determination supported by execution
evidence, not an inference from GitHub status. It records job success and may release
ready dependent work once. `aborted` records the execution as failed, revokes its
callback token, and fails unstarted dependents; any further apply requires a fresh
reviewed plan through the normal workflow. It does not roll back resources.

Resolution, job/token changes, batch updates, and dependent work commit in one
fenced transaction. Retry the same resolution ID, body, and authenticated principal
after a lost response. The same decision returns its stored record; a different
decision or evidence conflicts with 409. A stale writer returns 503. History cannot
be overwritten or deleted through the application or ordinary table writes.
Transient provider errors keep retrying; they do not become proof of permanent
absence. An expired execution's recovery record is committed before the provider
lookup, so deleted runs, malformed responses, and prolonged provider failures
cannot make the operator recovery API disappear.

## Validation

PostgreSQL tests cover natural expiry across row-lock waits, callback/grant replay,
concurrent resolution, unavailable runs, inactive installations, tenant isolation,
and the actual migration's immutability triggers. Migration rehearsals and runtime
enablement are separate gates; these tests are not production cutover proof.
