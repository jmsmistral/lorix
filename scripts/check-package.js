import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readmeExamples } from './readme-examples.js';
import { exampleProgram } from '../test/readme-cases.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const directory = await mkdtemp(path.join(os.tmpdir(), 'lorix-package-'));
const npmCli = process.env.npm_execpath;
assert.ok(npmCli, 'Run this check with npm run test:package.');

function run(args, cwd) {
  const result = spawnSync(process.execPath, [npmCli, ...args], {
    cwd,
    encoding: 'utf8',
    timeout: 60000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return result.stdout;
}

try {
  const [packed] = JSON.parse(
    run(
      ['pack', '--json', '--ignore-scripts', '--pack-destination', directory],
      root,
    ),
  );
  const paths = packed.files.map((file) => file.path);
  for (const required of [
    'lorix.js',
    'package.json',
    'README.md',
    'LICENSE',
    'src/dataframe.js',
  ])
    assert.ok(paths.includes(required), `Missing ${required}`);
  for (const file of paths)
    assert.match(
      file,
      /^(lorix\.js|package\.json|README\.md|LICENSE|src\/[^/]+\.js|docs\/images\/lorix\.png)$/,
    );

  await writeFile(
    path.join(directory, 'package.json'),
    JSON.stringify({ private: true, type: 'module' }),
  );
  // Install as a consumer would, without access to the source checkout or its
  // dev dependencies. Use cached downloads where possible.
  run(
    [
      'install',
      path.join(directory, packed.filename),
      '--prefer-offline',
      '--omit=dev',
      '--ignore-scripts',
      '--no-audit',
      '--no-fund',
      '--no-package-lock',
    ],
    directory,
  );
  const examples = await readmeExamples();
  assert.ok(examples.length > 0);
  for (const example of examples) {
    const result = spawnSync(process.execPath, ['--input-type=module'], {
      cwd: directory,
      input: exampleProgram(example),
      encoding: 'utf8',
      timeout: 10000,
    });
    assert.ifError(result.error);
    assert.equal(
      result.status,
      0,
      `Packaged README example at line ${example.line}: ${result.stderr}`,
    );
  }
  console.log(
    `Verified ${paths.length} packaged files and ${examples.length} README examples against an isolated installation.`,
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
