# Development workflow

Start with [the project catalog](docs/PROJECT_CATALOG.md). Each application owns its dependencies and configuration; the repository is a collection, not one deployable application.

1. Create a branch for your change.
2. Enter the relevant project folder and follow its README.
3. Keep real credentials in local environment files. Do not commit user uploads, runtime records, database exports or Terraform state.
4. Run `python scripts/check_projects.py` from the repository root. This checks manifests and source syntax, not behavior, JSX or TypeScript builds.
5. Run the affected application's tests or build. Record the command and result in your change description. Use mock providers or isolated test accounts; never production funds or customer records.
6. Update setup documentation when commands, dependencies or behavior change.

The root GitHub Actions workflow runs source checks, banking tests, the Trace API health test and the storefront build. A passing workflow does not verify deployments, financial correctness, security or production readiness.

Use separate pull requests for unrelated applications. Do not rewrite project ownership, add performance metrics or claim production use without evidence. Demo credentials and generated data must remain clearly identified as such.
