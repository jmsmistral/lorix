import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import lorix from '../lorix.js';

const frame = (rows) => lorix.DataFrame.fromArray(rows);
const values = () => frame([{ v: 10 }, { v: 20 }, { v: 30 }]);

describe('Data integrity regressions', () => {
  it('keeps distinct tuples with commas and distinct scalar types', () => {
    const rows = [
      { a: 'x,y', b: 'z' },
      { a: 'x', b: 'y,z' },
    ];
    assert.deepEqual(frame(rows).distinct().rows, rows);
    const mixed = [1, '1', null, undefined, '', false, 0, NaN, NaN].map(
      (v) => ({ v }),
    );
    assert.equal(frame(mixed).distinct().rows.length, 8);
  });

  for (const method of ['replace', 'replaceAll', 'regexReplace']) {
    it(`${method} preserves non-string cells and its input`, () => {
      const rows = [{ v: 'aa' }, { v: null }, { v: 42 }, { v: undefined }];
      const before = structuredClone(rows);
      const result = frame(rows)[method](
        ['v'],
        method === 'regexReplace' ? /a/g : 'a',
        'b',
      );
      assert.deepEqual(result.rows, [
        { v: method === 'replace' ? 'ba' : 'bb' },
        ...rows.slice(1),
      ]);
      assert.deepEqual(rows, before);
    });
  }

  it('retains a unique schema for an empty join', () => {
    const result = frame([{ id: 1, a: 'x' }]).innerJoin(
      frame([{ id: 2, b: 'y' }]),
      ['id'],
    );
    assert.deepEqual(result.columns, ['id', 'a', 'b']);
    assert.deepEqual(result.size(), [0, 3]);
  });

  it('rejects cross-side join-key collisions instead of overwriting data', () => {
    const left = frame([{ id: 1, key: 'keep' }]);
    const right = frame([{ key: 1, v: 'right' }]);
    for (const method of [
      'innerJoin',
      'leftJoin',
      'rightJoin',
      'fullOuterJoin',
      'leftAntiJoin',
      'rightAntiJoin',
    ]) {
      assert.throws(() => left[method](right, ['id'], ['key']), /overlap/i);
    }
  });

  it('evaluates predicate methods on real cell values, once per pair', () => {
    const left = frame([{ a: 'ABC', n: 1, date: new Date('2023-01-01') }]);
    const right = frame([{ b: 'abc' }]);
    let calls = 0;
    const result = left.innerJoin(right, (l, r) => {
      calls++;
      return (
        l.a.toLowerCase() === r.b &&
        l.n.toFixed(0) === '1' &&
        l.date.getUTCFullYear() === 2023
      );
    });
    assert.equal(result.rows.length, 1);
    assert.equal(calls, 1);
  });

  it('rejects unequal common values in predicate joins', () => {
    assert.throws(
      () =>
        frame([{ id: 1 }]).innerJoin(frame([{ id: 2 }]), (l, r) => l.id < r.id),
      /overlap/i,
    );
  });

  it('uses consistent cross-join suffixes even with no rows', () => {
    const result = new lorix.DataFrame([], ['id']).crossJoin(
      frame([{ id: 1 }]),
    );
    assert.deepEqual(result.columns, ['id_x', 'id_y']);
    assert.throws(
      () => frame([{ id: 1, id_x: 2 }]).crossJoin(frame([{ id: 3 }])),
      /collision|overlap/i,
    );
  });
});

describe('Window regressions', () => {
  it('includes the final row when following bounds exceed the partition', () => {
    const result = values().withColumn(
      's',
      lorix.window(lorix.sum('v'), [], [], [lorix.currentRow, 2]),
    );
    assert.deepEqual(
      result.rows.map((r) => r.s),
      [60, 50, 30],
    );
  });

  it('uses exact lag and lead offsets, including zero', () => {
    for (const [fn, expected] of [
      [lorix.lag('v', 2), [null, null, 10]],
      [lorix.lead('v', 2), [30, null, null]],
      [lorix.lag('v', 0), [10, 20, 30]],
      [lorix.lead('v', 0), [10, 20, 30]],
    ]) {
      assert.deepEqual(
        values()
          .withColumn('out', lorix.window(fn))
          .rows.map((r) => r.out),
        expected,
      );
    }
  });

  it('does not alter input rows or expose temporary properties to callbacks', () => {
    const df = values();
    const before = structuredClone(df.rows);
    for (const row of df.rows) Object.freeze(row);
    const result = df.withColumn(
      's',
      lorix.window(
        (rows) => {
          for (const row of rows) assert.deepEqual(Object.keys(row), ['v']);
          return rows.length;
        },
        [],
        [],
        [1, lorix.currentRow],
      ),
    );
    assert.deepEqual(
      result.rows.map((r) => r.s),
      [1, 2, 2],
    );
    assert.deepEqual(df.rows, before);
    assert.doesNotThrow(() => df.withColumn('normal', () => 1));
  });

  it('supports multiple partition columns and replacing an existing column', () => {
    const df = frame([
      { a: 'x', b: 'y', v: 10 },
      { a: 'x', b: 'z', v: 40 },
      { a: 'x', b: 'y', v: 20 },
    ]);
    const result = df.withColumn('v', lorix.window(lorix.sum('v'), ['a', 'b']));
    assert.deepEqual(
      result.rows.map((r) => r.v),
      [30, 40, 30],
    );
    assert.deepEqual(result.columns, df.columns);
  });

  it('rejects invalid frames, offsets and aggregation columns', () => {
    for (const bounds of [[1], [-1, 1], [0.5, 1], [1, 'invalid']]) {
      assert.throws(
        () =>
          values().withColumn(
            's',
            lorix.window(lorix.sum('v'), [], [], bounds),
          ),
        /window|bound/i,
      );
    }
    assert.throws(
      () => values().withColumn('s', lorix.window(lorix.sum('typo'))),
      /column/i,
    );
    assert.throws(() => lorix.lag('v', -1), /offset/i);
    assert.throws(() => lorix.lead('v', 0.5), /offset/i);
  });

  it('supports documented exports while retaining existing aliases', () => {
    assert.equal(lorix.stdev, lorix.stddev);
    assert.equal(lorix.unboundedProceeding, lorix.unboundedProceding);
    assert.equal(typeof lorix.unboundedProceeding, 'string');
  });
});

describe('Pivot regressions', () => {
  it('counts original rows independently for each category', () => {
    const df = frame([
      { g: 'a', p: 'x', v: 2 },
      { g: 'a', p: 'x', v: 3 },
      { g: 'a', p: 'y', v: 4 },
    ]);
    assert.deepEqual(df.pivot(['g'], 'p', 'v', 'count').rows, [
      { g: 'a', x_count: 2, y_count: 1 },
    ]);
  });

  it('supports numeric categories with string column names', () => {
    const df = frame([
      { g: 'a', p: 2020, v: 2 },
      { g: 'a', p: 2021, v: 3 },
    ]);
    const result = df.pivot(['g'], 'p', 'v', 'sum');
    assert.deepEqual(result.columns, ['g', '2020_sum', '2021_sum']);
    assert.deepEqual(result.rows, [{ g: 'a', '2020_sum': 2, '2021_sum': 3 }]);
  });
});

describe('Export regressions', () => {
  let directory;
  beforeEach(async () => {
    directory = await mkdtemp(path.join(os.tmpdir(), 'lorix-test-'));
  });
  afterEach(async () => {
    await rm(directory, { recursive: true, force: true });
  });

  for (const [write, read, delimiter] of [
    [lorix.writeCsv, lorix.readCsv, ','],
    [lorix.writeTsv, lorix.readTsv, '\t'],
    [lorix.writeDsv, lorix.readDsv, '|'],
  ]) {
    it(`round-trips quoted ${JSON.stringify(delimiter)} data at an absolute path`, async () => {
      const file = path.join(directory, 'data.txt');
      const df = frame([
        { a: `one${delimiter}two`, b: 'line\n"quoted"' },
        { a: 'plain', b: '' },
      ]);
      await write(df, file, delimiter);
      const result = await read(file, delimiter);
      assert.deepEqual(result.rows, [
        { ...df.rows[0] },
        { a: 'plain', b: null },
      ]);
    });

    it(`preserves empty schemas in ${JSON.stringify(delimiter)} exports`, async () => {
      const file = path.join(directory, 'empty.txt');
      await write(new lorix.DataFrame([], ['a', 'b']), file, delimiter);
      assert.equal(await readFile(file, 'utf8'), `a${delimiter}b`);
      assert.deepEqual((await read(file, delimiter)).columns, ['a', 'b']);
    });
  }
});
