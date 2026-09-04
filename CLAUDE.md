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
- The root is not a selected `go.work` module, so `go test ./...` fails there.
  Run it from each affected module directory, such as `backend`, `libs`, `cli`,
  or `ee/cli`.
- This repository does not provide `scripts/check.sh`; run the affected module
  suites locally and use the existing CI jobs for repository-wide validation.
- The full `libs` and `cli` suites include integration tests that require
  GitHub App credentials, Terraform, a licence service, Azurite, Google ADC,
  and AWS credentials. Run focused affected packages locally and use the
  repository CI jobs for the credentialled integration suites.
