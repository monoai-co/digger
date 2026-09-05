# Repository tooling

- Atlas is not configured as a persistent mise tool. Run the pinned migration
  CLI with `mise x aqua:ariga/atlas@0.37.0 -- atlas ...`.
- Atlas 0.37 selects an apply endpoint with `atlas migrate apply --to-version
  <version>`; the shorter `--to` flag is not supported.
- Atlas environment schema URLs require the matching environment and attribute:
  use `--env gorm --to env://src` for the GORM source in `backend/atlas.hcl`.
- Set `GOWORK=off` when loading the backend GORM schema through Atlas because
  the provider invokes `go run -mod=mod`, which is incompatible with workspace
  mode in this repository.
- Atlas `schema diff --include` requires Atlas Pro. Run the complete schema
  diff for migration parity instead of filtering tables with that flag.
- The GORM source represents a database. Omit `search_path` from the PostgreSQL
  URL when comparing it; a schema-scoped URL cannot be diffed against that source.
- The root is not a selected `go.work` module, so `go test ./...` fails there.
  Run it from each affected module directory, such as `backend`, `libs`, `cli`,
  or `ee/cli`.
- This repository does not provide `scripts/check.sh`; run the affected module
  suites locally and use the existing CI jobs for repository-wide validation.
- For GORM relations whose foreign and referenced field names coincide, declare
  `belongsTo` explicitly. Otherwise AutoMigrate can infer the reverse foreign key;
  verify both PostgreSQL migration output and the SQLite test schema.
- The full `libs` and `cli` suites include integration tests that require
  GitHub App credentials, Terraform, a licence service, Azurite, Google ADC,
  and AWS credentials. Run focused affected packages locally and use the
  repository CI jobs for the credentialled integration suites.
