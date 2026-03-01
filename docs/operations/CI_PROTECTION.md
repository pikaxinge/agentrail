# CI Protection

This repository ships a required CI workflow at:

- `.github/workflows/ci.yml`

The workflow publishes three checks on every pull request:

- `CI / fmt`
- `CI / check`
- `CI / test`

## Enable Branch Protection (GitHub UI)

1. Open repository `Settings -> Branches`.
2. Add branch protection rule for `main`.
3. Enable:
- `Require a pull request before merging`
- `Require status checks to pass before merging`
- `Require branches to be up to date before merging`
4. Select required checks:
- `CI / fmt`
- `CI / check`
- `CI / test`
5. Optional hardening:
- `Require conversation resolution before merging`
- `Restrict who can push to matching branches`
- `Do not allow bypassing the above settings`

## Notes

- Required checks appear after the first PR run completes.
- If check names ever change in workflow, update branch protection required-check list accordingly.
