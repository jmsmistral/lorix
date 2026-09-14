# Contributing

Use Node.js 22.13+ or Node.js 24 LTS. Install the locked dependency tree with `npm ci`.

- `npm test` runs the unit tests and every README JavaScript example.
- `npm run test:coverage` checks at least 90% lines, statements, and functions, and 85% branches. Reports are in `coverage/`.
- `npm run lint` checks JavaScript, including tests and scripts.
- `npm run format` applies formatting; `npm run format:check` checks without changing files.
- `npm run test:package` packs the module, checks its contents, installs it into a temporary consumer, and runs the README examples there. It reuses cached downloads, requires registry access for uncached dependency metadata, and cleans up afterward.
- `npm run check` runs all of the above checks required by CI.

For a bug fix, add a regression test that fails before the fix. Prefer small fixtures with explicit expected values. For joins and windows, include empty inputs, nulls, duplicate keys, and partition boundaries as appropriate. Use temporary directories for I/O tests and clean them up in `finally` blocks or test hooks. Keep callbacks pure; Lorix does not deep-freeze consumer data.

README JavaScript blocks must be standalone ES modules. Include assertions for the behavior being demonstrated. The test harness executes complete blocks, so alternatives must use distinct variables or separate blocks.

Keep public method signatures compatible when possible. Document intentional behavior changes in `CHANGELOG.md`. Do not change existing expected results merely to make a failing test pass; establish the intended result independently first.

## Dependencies and CI

CI runs the full checks on Linux with Node 22 and 24, plus Windows with Node 24. GitHub Actions are pinned to commits. Dependabot proposes monthly npm and Action updates. Review dependency changes and run `npm audit`; evaluate reachability rather than assuming every advisory is exploitable. The CI audit fails on high or critical advisories, including development dependencies.

## Release process

1. Finish the changes and run `npm run check` and `npm audit`.
2. Choose the next version using semantic versioning. The current unreleased changes raise the Node minimum and tighten invalid-input handling, so use a minor release while the package is below 1.0.
3. Run `npm version <version> --no-git-tag-version`, update the changelog, and commit both manifest and lockfile with the code.
4. Publish a GitHub release tagged `v<version>` at the reviewed commit. Use a prerelease version and prerelease release for preview packages.
5. The publish workflow verifies the tag against the manifest, runs the complete checks, audits dependencies, and publishes the package. Stable releases use the `latest` npm tag; prereleases use `next`.

Publishing requires the repository's existing `npm_token` secret to contain a valid npm write credential for this package. This workflow does not configure npm credentials or publish on ordinary pushes. Never commit credentials. If publishing fails, inspect the run before retrying; npm versions cannot be overwritten.
