import assert from 'node:assert/strict';

const standardRows = [
  { colA: 1, colB: 2 },
  { colA: 2, colB: 3 },
  { colA: 3, colB: 4 },
];
const namedJoinRows = [
  { colA: 1, colC: 10 },
  { colA: 2, colC: 20 },
];
const otherJoinRows = [
  { colB: 1, colD: 10 },
  { colB: 3, colD: 30 },
];

// Fixtures and expectations are separate from the documentation. The harness
// executes each complete README block unchanged apart from lifting its import.
export const readmeCases = {
  'create-a-dataframe:1': {
    declaresFrames: true,
    setup: `
      await writeFile('test.csv', 'colA,colB\\n1,2\\n2,3\\n3,4');
      await writeFile('test.tsv', 'colA\\tcolB\\n1\\t2\\n2\\t3\\n3\\t4');
      await writeFile('test.psv', 'colA|colB\\n1|2\\n2|3\\n3|4');
    `,
    verify: `for (const result of [df1, df2, df3, df4]) assert.deepEqual(result.rows, fixtureRows);`,
  },
  'print-top-n-rows:1': {
    verify: `assert.deepEqual(printedTables, [[fixtureRows, ['colA', 'colB']], [fixtureRows, ['colA', 'colB']]]);`,
  },
  'iterate-dataframe-rows-like-an-object-array:1': {
    verify: `assert.deepEqual(df, fixtureRows);`,
  },
  'export-dataframe-rows-as-an-object-array:1': {
    verify: `assert.deepEqual(rowArray, fixtureRows);`,
  },
  'select-columns:1': {
    verify: `assert.deepEqual(df.rows, fixtureRows); assert.deepEqual(df.columns, ['colA', 'colB']);`,
  },
  'drop-columns:1': {
    verify: `assert.deepEqual(df.rows, [{colB: 2}, {colB: 3}, {colB: 4}]);`,
  },
  'define-new-column:1': {
    verify: `assert.deepEqual(df.rows.map(row => row.newCol), [3, 5, 7]);`,
  },
  'define-new-column:2': {
    verify: `
      assert.deepEqual(constantDf.rows.map(row => row.newCol), [3, 3, 3]);
      assert.ok(dateDf.rows.every(row => row.newCol instanceof Date && Number.isFinite(row.newCol.valueOf())));
    `,
  },
  'define-new-column:3': {
    verify: `assert.deepEqual(df.rows.map(row => [row.newCol, row.newCol2]), [[1,2],[1,2],[1,2]]);`,
  },
  'filter-rows:1': {
    rows: [{ colA: 1 }, { colA: 11 }, { colA: 20 }],
    verify: `assert.deepEqual(df.rows, [{colA:11}, {colA:20}]);`,
  },
  'replace-strings:1': {
    rows: [
      { colA: 'oldSubstring-oldSubstring', colB: 'OLDSUBSTRING' },
      { colA: null, colB: 'plain' },
    ],
    verify: `
      assert.deepEqual(replacedDf.rows, [{colA:'newSubstring-oldSubstring',colB:'OLDSUBSTRING'}, {colA:null,colB:'plain'}]);
      assert.deepEqual(replacedAllDf.rows, [{colA:'newSubstring-newSubstring',colB:'OLDSUBSTRING'}, {colA:null,colB:'plain'}]);
      assert.deepEqual(regexDf.rows, [{colA:'newSubstring-newSubstring',colB:'newSubstring'}, {colA:null,colB:'plain'}]);
    `,
  },
  'drop-duplicate-rows:1': {
    rows: [
      { colA: 1, colB: 2, colC: 3 },
      { colA: 1, colB: 2, colC: 4 },
      { colA: 1, colB: 2, colC: 3 },
    ],
    verify: `assert.deepEqual(distinctDf.rows, fixtureRows.slice(0,2)); assert.deepEqual(distinctSubsetDf.rows, fixtureRows.slice(0,1));`,
  },
  'sorting:1': {
    rows: [
      { colA: 2, colB: 1 },
      { colA: 1, colB: 2 },
      { colA: 1, colB: 3 },
    ],
    verify: `
      assert.deepEqual(sortedDf.rows, [fixtureRows[1],fixtureRows[2],fixtureRows[0]]);
      assert.deepEqual(multiSortDf.rows, [fixtureRows[2],fixtureRows[1],fixtureRows[0]]);
      assert.throws(() => df1.orderBy('id'), /array/i);
    `,
  },
  'joining-dataframes:1': {
    rows: [{ colA: 1 }],
    other: [{ colA: 2 }, { colA: 3 }],
    verify: `assert.deepEqual(df.rows, [{colA_x:1,colA_y:2},{colA_x:1,colA_y:3}]);`,
  },
  'joining-dataframes:2': {
    rows: [
      { colA: 1, colB: 2 },
      { colA: 2, colB: 3 },
    ],
    other: [
      { colA: 1, colB: 2 },
      { colA: 3, colB: 4 },
    ],
    verify: `assert.deepEqual(innerDf.rows, [{colA:1,colB:2}]); assert.deepEqual(outerDf.rows, [{colA:1,colB:2},{colA:2,colB:3},{colA:3,colB:4}]);`,
  },
  'joining-dataframes:3': {
    rows: namedJoinRows,
    other: otherJoinRows,
    verify: `assert.deepEqual(df.rows, [{colA:1,colC:10,colB:1,colD:10}]);`,
  },
  'joining-dataframes:4': {
    rows: namedJoinRows,
    other: otherJoinRows,
    verify: `
      assert.deepEqual(innerDf.rows, [{colA:1,colC:10,colB:1,colD:10}]);
      assert.deepEqual(multiKeyDf.rows, innerDf.rows);
      assert.deepEqual(leftDf.rows, [{colA:1,colC:10,colB:1,colD:10},{colA:2,colC:20,colB:null,colD:null}]);
      assert.deepEqual(rightDf.rows, [{colA:null,colC:null,colB:1,colD:10},{colA:null,colC:null,colB:3,colD:30}]);
      assert.deepEqual(leftAntiDf.rows, [{colA:2,colC:20,colB:null,colD:null}]);
      assert.deepEqual(rightAntiDf.rows, rightDf.rows);
    `,
  },
  'aggregating-with-groupby:1': {
    rows: [
      { colA: 'a', colB: 'b', colC: 2, colD: 3, colE: 4 },
      { colA: 'a', colB: 'b', colC: 4, colD: 5, colE: 6 },
    ],
    verify: `assert.deepEqual(df.rows, [{colA:'a',colB:'b',colC_sum:6,colC_mean:3,colC_count:2,colD_sum:8,colE_min:4,colE_max:6}]);`,
  },
  'window-functions:1': {
    rows: [
      { colA: 'a', colB: 1, colX: 10 },
      { colA: 'a', colB: 2, colX: 20 },
      { colA: 'a', colB: 3, colX: 30 },
    ],
    verify: `
      assert.deepEqual(df.rows.map(row => row.colB), [3,2,1]);
      assert.equal(df.rows[0].colStddev, null);
      assert.ok(Math.abs(df.rows[1].colStddev - Math.sqrt(50)) < 1e-12);
      assert.equal(df.rows[2].colStddev, 10);
    `,
  },
  'union-between-dataframes:1': {
    verify: `assert.deepEqual(df.rows, [...fixtureRows,...fixtureOtherRows]);`,
  },
  'pivot-values-into-columns:1': {
    rows: [
      { category: 'a', colour: 'red', amount: 2 },
      { category: 'a', colour: 'red', amount: 3 },
      { category: 'a', colour: 'blue', amount: 4 },
    ],
    verify: `assert.deepEqual(df.rows, [{category:'a',red_sum:5,blue_sum:4}]);`,
  },
  'export-to-files:1': {
    verify: `
      assert.equal(await readFile('output.csv','utf8'), 'colA,colB\\n1,2\\n2,3\\n3,4');
      assert.equal(await readFile('output.tsv','utf8'), 'colA\\tcolB\\n1\\t2\\n2\\t3\\n3\\t4');
      assert.equal(await readFile('output.psv','utf8'), 'colA|colB\\n1|2\\n2|3\\n3|4');
      assert.deepEqual(JSON.parse(await readFile('output.json','utf8')), fixtureRows);
    `,
  },
};

export function exampleProgram(example, entryPoint = 'lorix') {
  const fixture = readmeCases[example.id];
  assert.ok(
    fixture?.verify,
    `Missing fixture or expected result for README example ${example.id}`,
  );
  const snippet = example.code.replace(
    /^import lorix from (['"])lorix\1;?\s*$/gm,
    '',
  );
  return `
    import assert from 'node:assert/strict';
    import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
    import os from 'node:os';
    import path from 'node:path';
    import lorix from ${JSON.stringify(entryPoint)};
    const directory = await mkdtemp(path.join(os.tmpdir(), 'lorix-readme-'));
    const originalDirectory = process.cwd();
    const fixtureRows = ${JSON.stringify(fixture.rows || standardRows)};
    const fixtureOtherRows = ${JSON.stringify(fixture.other || standardRows)};
    const printedTables = [];
    console.table = (...args) => printedTables.push(args);
    try {
      process.chdir(directory);
      ${fixture.declaresFrames ? '' : 'let df1 = lorix.DataFrame.fromArray(fixtureRows); let df2 = lorix.DataFrame.fromArray(fixtureOtherRows);'}
      ${fixture.setup || ''}
      ${snippet}
      ${fixture.verify}
    } finally {
      process.chdir(originalDirectory);
      await rm(directory, {recursive:true, force:true});
    }
  `;
}
