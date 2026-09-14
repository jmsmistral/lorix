import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const { version } = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8'),
);
assert.equal(
  process.env.RELEASE_TAG,
  `v${version}`,
  'Release tag must match package.json.',
);
assert.equal(
  process.env.IS_PRERELEASE,
  String(version.includes('-')),
  'Release prerelease status must match the package version.',
);
console.log(`Release metadata verified for v${version}.`);
