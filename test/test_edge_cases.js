import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import lorix from '../lorix.js';

const frame = (rows) => lorix.DataFrame.fromArray(rows);

describe('DataFrame boundaries', () => {
  it('rejects malformed rows and schemas with descriptive errors', () => {
    for (const rows of [null, 'rows', [null], [1], [['a']]]) {
      assert.throws(() => new lorix.DataFrame(rows), /array|object/i);
    }
    assert.throws(() => new lorix.DataFrame([], ['a', 'a']), /unique/i);
    assert.throws(() => new lorix.DataFrame([], [1]), /string/i);
    assert.throws(() => new lorix.DataFrame([{ a: 1 }], ['b']), /differences/i);
    assert.deepEqual(new lorix.DataFrame([{ a: 1 }]).columns, ['a']);
    assert.throws(() => frame([]), /non-empty/i);
    assert.throws(() => frame([{ a: 1 }, { b: 2 }]), /equal/i);
  });

  it('selects and drops literal column names and handles zero-column frames', () => {
    const data = frame([{ 'a.b': 1, a: { b: 2 } }]);
    assert.deepEqual(data.select('a.b').rows, [{ 'a.b': 1 }]);
    assert.deepEqual(data.drop('a.b').rows, [{ a: { b: 2 } }]);
    assert.deepEqual(data.drop('a.b', 'a').size(), [1, 0]);
    assert.equal(new lorix.DataFrame([{}, {}]).distinct().rows.length, 1);
  });

  it('preserves object identity distinctions during deduplication', () => {
    const shared = { id: 1 };
    const data = frame([{ v: shared }, { v: { id: 1 } }, { v: shared }]);
    assert.equal(data.distinct().rows.length, 2);
    assert.equal(data.distinct(['v']).rows[0].v, shared);
    assert.equal(new lorix.DataFrame([], ['v']).distinct().rows.length, 0);
  });

  it('checks both sides of a union schema even when there are no rows', () => {
    const left = new lorix.DataFrame([], ['a']);
    const right = new lorix.DataFrame([], ['a', 'b']);
    assert.throws(() => left.unionByName(right), /different columns/i);
    assert.throws(() => right.unionByName(left), /different columns/i);
  });
});

describe('Join boundaries', () => {
  const methods = [
    'innerJoin',
    'leftJoin',
    'rightJoin',
    'fullOuterJoin',
    'leftAntiJoin',
    'rightAntiJoin',
  ];
  for (const [method, count] of methods.map((name, i) => [
    name,
    [6, 8, 7, 9, 2, 1][i],
  ])) {
    it(`${method} preserves many-to-many multiplicity and null/NaN keys`, () => {
      const left = frame([1, 1, 2, 3, null, NaN].map((id, l) => ({ id, l })));
      const right = frame([1, 1, 4, null, NaN].map((id, r) => ({ id, r })));
      const indexed = left[method](right, ['id']);
      let calls = 0;
      const functional = left[method](right, (l, r) => {
        calls++;
        return Object.is(l.id, r.id);
      });
      assert.equal(indexed.rows.length, count);
      assert.deepEqual(indexed.rows, functional.rows);
      assert.equal(calls, 30);
    });
  }

  for (const method of methods) {
    it(`${method} handles empty inputs with stable schemas`, () => {
      const left = new lorix.DataFrame([], ['id', 'l']);
      const right = frame([{ id: 1, r: 'right' }]);
      const result = left[method](right, ['id']);
      const expected = ['rightJoin', 'fullOuterJoin', 'rightAntiJoin'].includes(
        method,
      )
        ? [{ id: 1, l: null, r: 'right' }]
        : [];
      assert.deepEqual(result.rows, expected);
      assert.deepEqual(result.columns, ['id', 'l', 'r']);
      assert.deepEqual(
        left[method](right, () => {
          throw Error('must not evaluate');
        }).rows,
        expected,
      );
    });
  }

  it('matches Date-valued indexed keys by timestamp without type coercion', () => {
    const left = frame([
      { id: new Date('2023-01-01'), l: 1 },
      { id: 1, l: 2 },
    ]);
    const right = frame([
      { id: new Date('2023-01-01'), r: 3 },
      { id: '1', r: 4 },
    ]);
    assert.equal(left.innerJoin(right, ['id']).rows.length, 1);
  });

  it('rejects unread shared payloads and preserves callback exceptions', () => {
    const left = frame([{ id: 1, payload: 'same' }]);
    const right = frame([{ id: 1, payload: 'same' }]);
    assert.throws(
      () => left.innerJoin(right, (l, r) => l.id === r.id),
      /overlap/i,
    );
    assert.throws(
      () =>
        left.innerJoin(right, () => {
          throw Error('application failure');
        }),
      /application failure/,
    );
  });
});

describe('Window boundaries', () => {
  const bounds = [0, 1, 10];
  for (const before of [...bounds, lorix.unboundedPreceding]) {
    for (const after of [...bounds, lorix.unboundedProceeding]) {
      it(`computes inclusive frames for [${before}, ${after}]`, () => {
        const input = frame([
          { g: 'a', v: 2 },
          { g: 'b', v: 7 },
          { g: 'a', v: 3 },
          { g: 'a', v: 5 },
        ]);
        const result = input.withColumn(
          'sum',
          lorix.window(lorix.sum('v'), ['g'], [], [before, after]),
        );
        // Independent reference: membership by relative position, not slicing.
        const expected = input.rows.map((row) => {
          const partition = input.rows.filter((other) => other.g === row.g);
          const position = partition.indexOf(row);
          return partition.reduce((sum, other, index) => {
            const included =
              (before === lorix.unboundedPreceding ||
                position - index <= before) &&
              (after === lorix.unboundedProceeding ||
                index - position <= after);
            return included ? sum + other.v : sum;
          }, 0);
        });
        assert.deepEqual(
          result.rows.map((row) => row.sum),
          expected,
        );
      });
    }
  }

  it('handles repeated row references, sorting, and empty partitions', () => {
    const row = { g: 'a', v: 2 };
    const input = frame([row, row, { g: 'b', v: 1 }]);
    const result = input.withColumn(
      'n',
      lorix.window(lorix.rownumber(), ['g'], [['v'], ['desc']]),
    );
    assert.deepEqual(
      result.rows.map((r) => r.n),
      [1, 2, 1],
    );
    assert.deepEqual(Object.keys(row), ['g', 'v']);
    const empty = new lorix.DataFrame([], ['g', 'v']);
    for (const partition of [[], ['g']]) {
      const output = empty.withColumn(
        'sum',
        lorix.window(lorix.sum('v'), partition),
      );
      assert.deepEqual(output.size(), [0, 3]);
    }
  });

  it('computes a full-partition callback once, including explicit full bounds', () => {
    const input = frame([{ v: 1 }, { v: 2 }]);
    let calls = 0;
    const output = input.withColumn(
      'size',
      lorix.window(
        (rows) => {
          calls++;
          return rows.length;
        },
        [],
        [],
        [lorix.unboundedPreceding, lorix.unboundedProceeding],
      ),
    );
    assert.equal(calls, 1);
    assert.deepEqual(
      output.rows.map((r) => r.size),
      [2, 2],
    );
  });

  it('calculates quantiles and rejects invalid probabilities', () => {
    const input = frame([{ v: 0 }, { v: 10 }, { v: 20 }]);
    assert.deepEqual(
      input
        .withColumn('q', lorix.window(lorix.quantile('v', 0.25)))
        .rows.map((r) => r.q),
      [5, 5, 5],
    );
    for (const p of [-1, 2, NaN, Infinity, '0.5'])
      assert.throws(() => lorix.quantile('v', p), /probability/i);
  });
});

describe('Aggregation boundaries', () => {
  const rows = [
    { g: 'a', p: 'x', v: null },
    { g: 'a', p: 'x', v: 4 },
    { g: 'b', p: 'y', v: 7 },
    { g: 'c', p: null, v: 9 },
  ];
  for (const [type, value, missing] of [
    ['sum', 4, 0],
    ['count', 2, 0],
    ['mean', 4, null],
    ['min', 4, null],
    ['max', 4, null],
  ]) {
    it(`pivots ${type} with null cells and absent categories`, () => {
      assert.deepEqual(frame(rows).pivot(['g'], 'p', 'v', type).rows, [
        { g: 'a', [`x_${type}`]: value, [`y_${type}`]: missing },
        {
          g: 'b',
          [`x_${type}`]: missing,
          [`y_${type}`]: type === 'count' ? 1 : 7,
        },
        { g: 'c', [`x_${type}`]: missing, [`y_${type}`]: missing },
      ]);
    });
  }

  it('rejects invalid aggregation specifications and output collisions', () => {
    const input = frame(rows);
    for (const spec of [
      {},
      null,
      [],
      { v: [] },
      { typo: 'sum' },
      { v: 'toString' },
    ]) {
      assert.throws(() => input.groupBy(['g'], spec), /aggregation|column/i);
    }
    assert.throws(() => input.pivot([], 'p', 'v', 'sum'), /column/i);
    assert.throws(
      () => input.pivot(['g'], 'p', 'v', 'invalid'),
      /aggregation/i,
    );
    assert.throws(
      () => frame([{ g: 1, p: {}, v: 2 }]).pivot(['g'], 'p', 'v', 'sum'),
      /categories/i,
    );
    assert.throws(
      () =>
        frame([
          { g: 1, p: 1, v: 2 },
          { g: 1, p: '1', v: 3 },
        ]).pivot(['g'], 'p', 'v', 'sum'),
      /collide/i,
    );
    assert.throws(
      () =>
        frame([{ x_sum: 1, p: 'x', v: 2 }]).pivot(['x_sum'], 'p', 'v', 'sum'),
      /collide/i,
    );
    assert.deepEqual(
      new lorix.DataFrame([], ['g', 'p', 'v'])
        .pivot(['g'], 'p', 'v', 'sum')
        .size(),
      [0, 1],
    );
  });
});

describe('I/O boundaries', () => {
  let directory;
  beforeEach(async () => {
    directory = await mkdtemp(path.join(os.tmpdir(), 'lorix-io-'));
  });
  afterEach(async () => {
    await rm(directory, { recursive: true, force: true });
  });

  it('supports relative paths and declared column order', async () => {
    const target = path.join(directory, 'out.csv');
    await lorix.writeCsv(
      new lorix.DataFrame([{ a: 1, b: 2 }], ['b', 'a']),
      path.relative(process.cwd(), target),
    );
    assert.equal(await readFile(target, 'utf8'), 'b,a\n2,1');
  });

  it('preserves filesystem errors and rejects invalid delimiters and frames', async () => {
    const target = path.join(directory, 'missing', 'out.csv');
    await assert.rejects(lorix.readCsv(target), { code: 'ENOENT' });
    await assert.rejects(lorix.writeCsv(frame([{ a: 1 }]), target), {
      code: 'ENOENT',
    });
    for (const delimiter of ['', '||', '\n', '"', 1]) {
      await assert.rejects(lorix.readDsv(target, delimiter), /delimiter/i);
      await assert.rejects(
        lorix.writeDsv(frame([{ a: 1 }]), target, delimiter),
        /delimiter/i,
      );
    }
    await assert.rejects(lorix.writeJson({}, target), /DataFrame/);
  });

  it('parses empty input and documents auto-typing', async () => {
    const target = path.join(directory, 'data.csv');
    await writeFile(target, '');
    assert.deepEqual((await lorix.readCsv(target)).size(), [0, 0]);
    await writeFile(target, 'id,flag,missing\n001,true,');
    assert.deepEqual((await lorix.readCsv(target)).rows, [
      { id: 1, flag: true, missing: null },
    ]);
  });
});
