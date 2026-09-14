import { group, sum, mean, min, max } from 'd3-array';
import { DataFrame } from './dataframe.js';
import { _groupLeaves, _isSubsetArray } from './utils.js';

const aggregations = {
  sum: (rows, col) => sum(rows, (row) => row[col]),
  mean: (rows, col) => mean(rows, (row) => row[col]) ?? null,
  min: (rows, col) => min(rows, (row) => row[col]) ?? null,
  max: (rows, col) => max(rows, (row) => row[col]) ?? null,
  count: (rows) => rows.length,
};

function aggregate(type, rows, col) {
  if (!Object.hasOwn(aggregations, type))
    throw Error(`Invalid aggregation: '${type}'.`);
  return aggregations[type](rows, col);
}

function validateOutputColumns(columns) {
  if (new Set(columns).size !== columns.length)
    throw Error('Aggregation output column names collide.');
}

export function groupAggregation(df, groupByCols, groupByAggs) {
  const groups = group(df.rows, ...groupByCols.map((col) => (row) => row[col]));
  if (groupByAggs === undefined) return groups;
  if (
    !groupByAggs ||
    typeof groupByAggs !== 'object' ||
    Array.isArray(groupByAggs) ||
    !Object.keys(groupByAggs).length
  ) {
    throw Error('groupBy() requires a non-empty aggregation object.');
  }
  const specifications = [];
  for (const [col, types] of Object.entries(groupByAggs)) {
    if (!df.columns.includes(col))
      throw Error(`Invalid aggregation column: '${col}'.`);
    const list = Array.isArray(types) ? types : [types];
    if (!list.length) throw Error('Aggregation lists must not be empty.');
    for (const type of list) {
      if (typeof type !== 'string' || !Object.hasOwn(aggregations, type))
        throw Error(`Invalid aggregation: '${type}'.`);
      specifications.push({ col, type, name: `${col}_${type}` });
    }
  }
  const columns = [...groupByCols, ...specifications.map((spec) => spec.name)];
  validateOutputColumns(columns);
  const rows = [..._groupLeaves(groups)].map((partition) =>
    Object.fromEntries([
      ...groupByCols.map((col) => [col, partition[0][col]]),
      ...specifications.map(({ col, type, name }) => [
        name,
        aggregate(type, partition, col),
      ]),
    ]),
  );
  return new DataFrame(rows, columns);
}

export function pivotAggregation(df, groupByCols, pivotCol, valueCol, aggType) {
  if (
    !Array.isArray(groupByCols) ||
    !_isSubsetArray(groupByCols, df.columns) ||
    !df.columns.includes(pivotCol) ||
    !df.columns.includes(valueCol)
  ) {
    throw Error('pivot() requires valid group, pivot and value columns.');
  }
  if (typeof aggType !== 'string' || !Object.hasOwn(aggregations, aggType))
    throw Error(`Invalid pivot aggregation: '${aggType}'.`);
  const categories = [
    ...new Set(
      df.rows
        .map((row) => row[pivotCol])
        .filter((value) => value !== null && value !== undefined),
    ),
  ];
  if (
    categories.some(
      (value) =>
        !['string', 'number', 'boolean', 'bigint'].includes(typeof value),
    )
  ) {
    throw Error(
      'Pivot categories must be strings, numbers, booleans or bigints.',
    );
  }
  const names = categories.map((value) => `${value}_${aggType}`);
  const columns = [...groupByCols, ...names];
  validateOutputColumns(columns);
  const groups = group(df.rows, ...groupByCols.map((col) => (row) => row[col]));
  const rows = [..._groupLeaves(groups)].map((partition) => {
    const byCategory = group(partition, (row) => row[pivotCol]);
    return Object.fromEntries([
      ...groupByCols.map((col) => [col, partition[0][col]]),
      ...categories.map((category, i) => [
        names[i],
        aggregate(aggType, byCategory.get(category) || [], valueCol),
      ]),
    ]);
  });
  return new DataFrame(rows, columns);
}
