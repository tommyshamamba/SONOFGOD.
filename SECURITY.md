# Security policy

These projects are demonstrations. Current verification and known limitations are documented in `docs/LOCAL_DEMO_VERIFICATION.md` and subsequent repair reports. Use synthetic data for local demonstrations.

## Reporting

If GitHub private vulnerability reporting is available on the Security tab, use **Report a vulnerability**. Do not put passwords, tokens, personal data or a working exploit against a live service in a public issue. If private reporting is unavailable, open a minimal issue requesting a private reporting channel without including sensitive details.

Reports should identify the affected project and commit, prerequisites, impact and a local reproduction using synthetic data. Do not test systems you do not own or have permission to assess.

## Maintenance

Dependency updates are configured in `.github/dependabot.yml`. The project checks workflow validates builds and tests; passing checks are not a security certification. Run each project's dependency audit before deployment and verify all deployment-specific controls. Live banking settlement, paid AI/telephony providers and production infrastructure require their own review.
