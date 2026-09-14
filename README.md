# Lorix

[![CI](https://github.com/jmsmistral/lorix/actions/workflows/lorix-pr-test.yml/badge.svg?branch=master)](https://github.com/jmsmistral/lorix/actions/workflows/lorix-pr-test.yml)

<img align="right" src="docs/images/lorix.png" height="110" alt="Lorix logo">

A small JavaScript DataFrame API for loading, joining, grouping, and transforming in-memory data. Lorix combines an array-of-objects interface with the aggregation and indexing tools in D3 and Lodash.

## Why Lorix?

I built Lorix to make small and medium-sized data-wrangling tasks convenient in JavaScript, without switching to Python. The focus is a composable API and readable transformations. Data stays in memory: this is not a streaming engine or a replacement for a database, and arbitrary predicate joins compare every pair of rows.

This repository contains the next, unreleased maintenance update. The npm release may not yet include these fixes. See [CHANGELOG.md](CHANGELOG.md) for compatibility notes.

## Install

Requires Node.js 22.13+ or 24+. CI checks the supported Node 22 and 24 LTS lines. Lorix uses ES modules; save examples as `.mjs` or set `"type": "module"` in your application's `package.json`.

```sh
npm install lorix
```

## Quick start

Every JavaScript block below is a standalone, executable example with assertions. CI runs them against both the source and an installed npm tarball.

```javascript
import assert from 'node:assert/strict';
import lorix from 'lorix';

const sales = lorix.DataFrame.fromArray([
  { team: 'North', units: 2, price: 10 },
  { team: 'South', units: 1, price: 15 },
  { team: 'North', units: 3, price: 10 },
]);

const totals = sales
  .withColumn('revenue', (row) => row.units * row.price)
  .groupBy(['team'], { revenue: 'sum' })
  .orderBy(['revenue_sum'], ['desc']);

assert.deepEqual(totals.toArray(), [
  { team: 'North', revenue_sum: 50 },
  { team: 'South', revenue_sum: 15 },
]);
totals.head();
```

## Rows, columns, and transformations

`fromArray()` requires a non-empty array of objects with the same own enumerable properties. Use `new lorix.DataFrame([], ['column'])` to construct an empty frame with a schema.

| Method                                    | Behavior                                                                              |
| ----------------------------------------- | ------------------------------------------------------------------------------------- |
| `size()`                                  | Returns `[rowCount, columnCount]`.                                                    |
| `head(n = 10)`                            | Prints the first `n` rows using `console.table`.                                      |
| `toArray()` / iteration                   | Exposes the row array / iterates its rows.                                            |
| `slice(i = 0, j = -1)`                    | Selects rows; `j` is **inclusive**, and `-1` means through the end.                   |
| `select(...columns)` / `drop(...columns)` | Keeps / removes named columns.                                                        |
| `withColumn(name, fn)`                    | Adds or replaces a column using `fn(row)`.                                            |
| `filter(fn)`                              | Keeps rows for which `fn(row)` is truthy.                                             |
| `distinct(subset = [])`                   | Keeps the first occurrence of each selected tuple; an empty subset means all columns. |
| `orderBy(columns, orders?)`               | Sorts by columns with `asc` (default) or `desc` directions.                           |
| `unionByName(other)`                      | Concatenates rows; column sets must match.                                            |

Transforms return new DataFrames and do not themselves modify source rows. They are **not deeply immutable**: construction and `toArray()` retain references, and operations such as filtering share row objects. Treat rows and nested values as read-only, including inside callbacks; copy data explicitly when independent ownership is needed. Ordinary callbacks follow JavaScript semantics for missing properties (`undefined`).

`distinct()` compares scalar values without coercion (`1` differs from `'1'`; `null` differs from `undefined`). It treats `NaN` values as equal and compares objects, arrays, and Dates by reference identity. Column selection uses literal names, including names containing dots.

```javascript
import assert from 'node:assert/strict';
import lorix from 'lorix';

const people = lorix.DataFrame.fromArray([
  { name: 'Ada', score: 9 },
  { name: 'Lin', score: 7 },
  { name: 'Ada', score: 9 },
]);
const selected = people
  .distinct()
  .filter((row) => row.score > 8)
  .select('name');
assert.deepEqual([...selected], [{ name: 'Ada' }]);
assert.deepEqual(people.slice(0, 1).size(), [2, 2]);
assert.deepEqual(people.drop('score').columns, ['name']);
assert.equal(selected.unionByName(selected).size()[0], 2);
assert.deepEqual(
  people
    .withColumn('score', (row) => row.score + 1)
    .rows.map((row) => row.score),
  [10, 8, 10],
);
```

String helpers accept an array of columns. `replace()` changes the first match, `replaceAll()` changes all matches, and `regexReplace()` uses a regular expression. Non-string cells, including `null`, pass through unchanged.

```javascript
import assert from 'node:assert/strict';
import lorix from 'lorix';

const names = lorix.DataFrame.fromArray([{ name: 'a-a' }, { name: null }]);
assert.deepEqual(names.replace(['name'], 'a', 'b').rows, [
  { name: 'b-a' },
  { name: null },
]);
assert.deepEqual(names.replaceAll(['name'], 'a', 'b').rows, [
  { name: 'b-b' },
  { name: null },
]);
assert.deepEqual(names.regexReplace(['name'], /a/g, 'b').rows, [
  { name: 'b-b' },
  { name: null },
]);
```

## Joins

`innerJoin`, `leftJoin`, `rightJoin`, `fullOuterJoin`, `leftAntiJoin`, and `rightAntiJoin` accept another frame and either:

- One array of shared key names: `left.innerJoin(right, ['id'])`.
- Two equally sized key arrays: `left.innerJoin(right, ['customerId'], ['id'])`.
- A predicate: `left.innerJoin(right, (l, r) => l.customerId === r.id)`.

Array joins use D3 InternMap indexing: primitive keys are not coerced, null keys can match, and object keys use `valueOf()` (for example, a Date uses its timestamp). Predicate joins evaluate real row pairs, respect short-circuiting, and support methods on cell values. Use `&&` and `||` for logical conditions. Missing columns throw only if the predicate actually accesses them; predicates are not evaluated on empty inputs.

Shared columns are coalesced only for corresponding same-name array keys, or when a predicate accesses them on both sides and matched values are equal. Other overlaps throw to prevent data loss; rename a column with `withColumn()` followed by `drop()` before joining. `crossJoin()` suffixes shared names with `_x` and `_y`, rejecting suffix collisions.

Non-matching outer rows receive `null` for columns available only on the other side. For compatibility, anti joins also include those null-filled columns. Left/inner/full joins traverse left rows; right joins traverse right rows. Full joins append unmatched right rows. Empty results retain their schema.

```javascript
import assert from 'node:assert/strict';
import lorix from 'lorix';

const orders = lorix.DataFrame.fromArray([
  { customerId: 1, amount: 20 },
  { customerId: 3, amount: 30 },
]);
const customers = lorix.DataFrame.fromArray([
  { id: 1, name: 'Ada' },
  { id: 2, name: 'Lin' },
]);
assert.deepEqual(orders.leftJoin(customers, ['customerId'], ['id']).rows, [
  { customerId: 1, amount: 20, id: 1, name: 'Ada' },
  { customerId: 3, amount: 30, id: null, name: null },
]);
assert.equal(
  orders
    .innerJoin(
      customers,
      (l, r) => l.customerId === r.id && r.name.startsWith('A'),
    )
    .size()[0],
  1,
);
assert.equal(
  orders.fullOuterJoin(customers, ['customerId'], ['id']).size()[0],
  3,
);
assert.equal(
  orders.leftAntiJoin(customers, ['customerId'], ['id']).rows[0].customerId,
  3,
);
assert.equal(orders.rightJoin(customers, ['customerId'], ['id']).size()[0], 2);
assert.equal(
  orders.rightAntiJoin(customers, ['customerId'], ['id']).rows[0].id,
  2,
);
assert.equal(orders.crossJoin(customers).size()[0], 4);
assert.equal(customers.innerJoin(customers, ['id', 'name']).size()[0], 2);
```

## Grouping and pivoting

`groupBy(columns, aggregations)` supports `sum`, `mean`, `count`, `min`, and `max`. Output names are `<column>_<aggregation>`. `count` counts rows, including rows with null values; numeric aggregates follow D3's missing-value handling. Without aggregations, `groupBy(columns)` returns a nested D3 `InternMap` of row groups.

`pivot(groupColumns, pivotColumn, valueColumn, aggregation)` produces `<category>_<aggregation>` columns. Categories retain first-seen order and may be strings, numbers, booleans, or bigints. Null/undefined categories are omitted. Missing categories yield `0` for sum/count and `null` for mean/min/max. Categories that produce the same output name (such as `1` and `'1'`) are rejected, as are collisions with group-column names.

```javascript
import assert from 'node:assert/strict';
import lorix from 'lorix';

const readings = lorix.DataFrame.fromArray([
  { site: 'A', sensor: 'temperature', value: 10 },
  { site: 'A', sensor: 'temperature', value: 20 },
  { site: 'A', sensor: 'humidity', value: 40 },
]);
assert.deepEqual(readings.groupBy(['site'], { value: ['sum', 'count'] }).rows, [
  { site: 'A', value_sum: 70, value_count: 3 },
]);
assert.equal(readings.groupBy(['site']).get('A').length, 3);
assert.deepEqual(readings.pivot(['site'], 'sensor', 'value', 'count').rows, [
  { site: 'A', temperature_count: 2, humidity_count: 1 },
]);
```

## Window functions

Use `withColumn(name, lorix.window(fn, partitionColumns?, order?, bounds?))` to calculate a value per row. Multiple partition columns are supported. `order` is `[columns]` or `[columns, directions]`, for example `[['time'], ['asc']]`. Explicit ordering sorts the output by partition columns and then the requested order; otherwise input row order is preserved.

Bounds are `[preceding, following]`, both inclusive of the current row. Each side accepts a non-negative integer, `lorix.currentRow`, or its corresponding `lorix.unboundedPreceding` / `lorix.unboundedProceeding` constant. An omitted or empty bounds array means the entire partition.

Available functions: `sum`, `min`, `max`, `mean`, `median`, `quantile(column, p = 0.5)`, `variance`, `stddev`, `lag(column, n)`, `lead(column, n)`, and `rownumber()`. Variance and standard deviation use sample statistics. Lag/lead require a non-negative integer offset and return `null` when that exact row is absent. Lag, lead, and rownumber determine their own bounds. Custom functions receive `(frameRows, partitionIndex)` for bounded windows, or `(partitionRows)` once per unbounded partition.

`stdev` aliases `stddev`, and the old misspelling `unboundedProceding` remains an alias of `unboundedProceeding`.

```javascript
import assert from 'node:assert/strict';
import lorix from 'lorix';

const readings = lorix.DataFrame.fromArray([
  { site: 'A', time: 1, value: 10 },
  { site: 'A', time: 2, value: 20 },
  { site: 'A', time: 3, value: 30 },
]);
const result = readings
  .withColumn(
    'running',
    lorix.window(
      lorix.sum('value'),
      ['site'],
      [['time']],
      [lorix.unboundedPreceding, lorix.currentRow],
    ),
  )
  .withColumn(
    'previous',
    lorix.window(lorix.lag('value', 2), ['site'], [['time']]),
  )
  .withColumn('deviation', lorix.window(lorix.stddev('value'), ['site']));
assert.deepEqual(
  result.rows.map((row) => row.running),
  [10, 30, 60],
);
assert.deepEqual(
  result.rows.map((row) => row.previous),
  [null, null, 10],
);
assert.deepEqual(
  result.rows.map((row) => row.deviation),
  [10, 10, 10],
);
assert.deepEqual(readings.columns, ['site', 'time', 'value']);
```

## Read and write files

`readCsv`, `readTsv`, and `readDsv(path, delimiter)` return promises for DataFrames. Parsing uses D3 `autoType`: numbers, booleans, and dates are inferred, empty cells become null, and numeric-looking identifiers may lose leading zeros. Use `fromArray()` with explicitly typed values when inference is inappropriate.

`writeCsv`, `writeTsv`, `writeDsv(frame, path, delimiter)`, and `writeJson` return promises. Text writers respect declared column order and retain headers for empty frames. Paths may be absolute or relative to the working directory. Writers replace existing files; parent directories must already exist. I/O failures reject with the underlying filesystem error. A custom delimiter must be one character other than a quote or newline.

```javascript
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import lorix from 'lorix';

const directory = await mkdtemp(path.join(os.tmpdir(), 'lorix-example-'));
try {
  const data = lorix.DataFrame.fromArray([{ name: 'Ada', score: 9 }]);
  for (const [write, read, delimiter] of [
    [lorix.writeCsv, lorix.readCsv, ','],
    [lorix.writeTsv, lorix.readTsv, '\t'],
    [lorix.writeDsv, lorix.readDsv, '|'],
  ]) {
    const file = path.join(directory, 'data.txt');
    await write(data, file, delimiter);
    assert.deepEqual((await read(file, delimiter)).rows, data.rows);
  }
  const jsonFile = path.join(directory, 'data.json');
  await lorix.writeJson(data, jsonFile);
  assert.deepEqual(JSON.parse(await readFile(jsonFile, 'utf8')), data.rows);
} finally {
  await rm(directory, { recursive: true, force: true });
}
```

## Development

```sh
npm ci
npm run check
npm run example
```

`npm run check` runs ESLint, Prettier checks, tests with coverage thresholds, and an isolated package-install check. See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow and release process. The implementation stays in JavaScript; TypeScript declarations and benchmarks are deferred.

## License

[GNU Affero General Public License v3.0](LICENSE).
