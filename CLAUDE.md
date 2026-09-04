# Repository tooling

- Atlas is not configured as a persistent mise tool. Run the pinned migration
  CLI with `mise x aqua:ariga/atlas@0.37.0 -- atlas ...`.
- The root is not a selected `go.work` module, so `go test ./...` fails there.
  Run it from each affected module directory, such as `backend`, `libs`, `cli`,
  or `ee/cli`.
- The full `libs` and `cli` suites include integration tests that require
  GitHub App credentials, Terraform, a licence service, Azurite, Google ADC,
  and AWS credentials. Run focused affected packages locally and use the
  repository CI jobs for the credentialled integration suites.
