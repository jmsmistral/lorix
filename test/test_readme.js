import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readmeExamples } from '../scripts/readme-examples.js';
import { exampleProgram, readmeCases } from './readme-cases.js';

const examples = await readmeExamples();
const entryPoint = new URL('../lorix.js', import.meta.url).href;

describe('README examples', () => {
  it('contains executable JavaScript examples', () =>
    assert.ok(examples.length > 0));
  it('has fixtures and expected results for every example', () => {
    assert.deepEqual(
      examples.map((example) => example.id).sort(),
      Object.keys(readmeCases).sort(),
    );
  });
  for (const example of examples) {
    it(`runs the complete code block at line ${example.line}`, function () {
      this.timeout(10000);
      const result = spawnSync(process.execPath, ['--input-type=module'], {
        input: exampleProgram(example, entryPoint),
        encoding: 'utf8',
        timeout: 8000,
      });
      assert.ifError(result.error);
      assert.equal(result.status, 0, result.stderr || result.stdout);
    });
  }
});
