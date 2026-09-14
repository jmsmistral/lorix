# Contributing

Use Node.js 22.x (22.13.0 or newer) or 24.x, matching the supported range in `package.json`. The latest Node.js 24 LTS release is recommended for development. Install the locked dependency tree with `npm ci`.

- `npm test` runs the unit tests and every README JavaScript example.
- `npm run test:coverage` checks at least 90% lines, statements, and functions, and 85% branches. Reports are in `coverage/`.
- `npm run lint` checks JavaScript, including tests and scripts.
- `npm run format` applies formatting; `npm run format:check` checks without changing files.
- `npm run test:package` packs the module, checks its contents, installs it into a temporary consumer, and runs the README examples there. It reuses cached downloads, requires registry access for uncached dependency metadata, and cleans up afterward.
- `npm run check` runs all of the above checks required by CI.

For a bug fix, add a regression test that fails before the fix. Prefer small fixtures with explicit expected values. For joins and windows, include empty inputs, nulls, duplicate keys, and partition boundaries as appropriate. Use temporary directories for I/O tests and clean them up in `finally` blocks or test hooks. Keep callbacks pure; Lorix does not deep-freeze consumer data.

Keep README examples short and focused on usage, with four-space indentation and no assertions or test setup. The README disables embedded-code formatting so the original four-space example layout remains intact. Fixtures and expected results belong in `test/readme-cases.js`; each JavaScript block is identified by its section heading and position within that section. The harness supplies input frames and temporary files, executes the full block, and checks its results against both the source and installed package. Alternatives within a block must use distinct variables or separate blocks.

Keep public method signatures compatible when possible. Document intentional behavior changes in `CHANGELOG.md`. Do not change existing expected results merely to make a failing test pass; establish the intended result independently first.

## Dependencies and CI

GitHub Actions runs on pushes and pull requests. CI runs the full checks on Linux with the minimum supported versions (22.13.0 and 24.0.0) and the latest Node 22 and 24 releases, plus Windows with the latest Node 24 release. When extending support, update the manifest and lockfile engines, CI matrix, and documentation together. The README badge tracks this workflow on `master`.

GitHub Actions are pinned to commits. Dependabot proposes monthly npm and Action updates. Review dependency changes and run `npm audit`; evaluate reachability rather than assuming every advisory is exploitable. The CI audit fails on high or critical advisories, including development dependencies.

## Release process

1. Finish the changes and run `npm run check` and `npm audit`.
2. Choose the next version using semantic versioning. Use a minor release while the package is below 1.0.
3. Run `npm version <version> --no-git-tag-version`, update the changelog, and commit both manifest and lockfile with the code.
4. Publish a GitHub release tagged `v<version>` at the reviewed commit. Use a prerelease version and prerelease release for preview packages.
5. The publish workflow verifies the tag against the manifest, runs the complete checks, audits dependencies, and publishes the package. Stable releases use the `latest` npm tag; prereleases use `next`.

Publishing uses npm trusted publishing (OIDC), without an npm token secret. In the `lorix` package settings on npmjs.com, configure a GitHub Actions trusted publisher with owner `jmsmistral`, repository `lorix`, workflow filename `lorix-publish.yml`, no environment name, and permission to publish directly with `npm publish`. See [npm's trusted publishing instructions](https://docs.npmjs.com/trusted-publishers/). This account setting must be configured separately; changing the workflow does not create it.

The publish job grants `id-token: write`, uses Node 24 and npm 11 with trusted publishing support, and disables dependency caching for releases. Ordinary pushes do not publish a package.

If publishing fails, first check `npm view lorix versions --json` to see whether the version reached npm. Published versions cannot be overwritten. If authentication or workflow changes are needed, merge the fix into `master`, then open **Actions → Publish → Run workflow**, select `master`, and enter the existing release tag (for example, `v0.4.0`). Select the prerelease option only for prerelease versions. The manual run uses the corrected workflow from `master` but checks out and validates the tagged source before running all checks and publishing. Do not move the release tag. Re-running an old failed run uses its original workflow, so it will not pick up a workflow fix merged afterward.
