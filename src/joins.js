import { group } from 'd3-array';
import lodash from 'lodash';
import { DataFrame } from './dataframe.js';

function requireDataFrame(df) {
  if (!(df instanceof DataFrame))
    throw Error('A join requires another DataFrame.');
}

function rejectOverlap(columns) {
  if (columns.length)
    throw Error(
      `Overlapping columns would lose data: '${columns.join(', ')}'. Rename or drop them before joining.`,
    );
}

export function _crossJoin(left, right) {
  requireDataFrame(right);
  const common = left.columns.filter((col) => right.columns.includes(col));
  // Preserve the historical ordering: non-common columns, then suffixed keys.
  const leftNames = [
    ...left.columns.filter((col) => !common.includes(col)),
    ...common,
  ];
  const rightNames = [
    ...right.columns.filter((col) => !common.includes(col)),
    ...common,
  ];
  const name = (col, side) => (common.includes(col) ? `${col}_${side}` : col);
  const columns = [
    ...leftNames.map((col) => name(col, 'x')),
    ...rightNames.map((col) => name(col, 'y')),
  ];
  if (new Set(columns).size !== columns.length)
    throw Error('Cross-join suffixes cause a column name collision.');
  const rows = left.rows.flatMap((l) =>
    right.rows.map((r) =>
      Object.fromEntries([
        ...leftNames.map((col) => [name(col, 'x'), l[col]]),
        ...rightNames.map((col) => [name(col, 'y'), r[col]]),
      ]),
    ),
  );
  return new DataFrame(rows, columns);
}

function validateKeys(left, right, leftKeys, rightKeys) {
  if (
    !Array.isArray(leftKeys) ||
    !Array.isArray(rightKeys) ||
    !leftKeys.length ||
    leftKeys.length !== rightKeys.length
  ) {
    throw Error('Join keys must be non-empty arrays of equal length.');
  }
  if (
    !leftKeys.every((col) => left.columns.includes(col)) ||
    !rightKeys.every((col) => right.columns.includes(col))
  ) {
    throw Error('Invalid columns in join keys.');
  }
  const sharedKeys = leftKeys.filter((col, i) => col === rightKeys[i]);
  rejectOverlap(
    left.columns.filter(
      (col) => right.columns.includes(col) && !sharedKeys.includes(col),
    ),
  );
}

function indexRows(df, columns) {
  return group(df.rows, ...columns.map((col) => (row) => row[col]));
}

function lookup(index, row, columns) {
  let node = index;
  for (const col of columns) {
    node = node.get(row[col]);
    if (!node) return [];
  }
  return node;
}

// Observe accesses during actual predicate evaluation. Cell values are returned
// unchanged, so string/number/Date methods work and callbacks are not probed.
function trackedRow(row, columns, accessed) {
  return new Proxy(row, {
    get(target, col, receiver) {
      if (typeof col === 'symbol') return Reflect.get(target, col, receiver);
      if (!columns.includes(col))
        throw Error(`Column '${col}' does not exist in DataFrame.`);
      accessed.add(col);
      return Reflect.get(target, col, receiver);
    },
  });
}

export function _join(type, left, right, on, leftOn, rightOn) {
  requireDataFrame(right);
  const predicate = typeof on === 'function' ? on : undefined;
  const leftKeys = on === undefined ? leftOn : on;
  const rightKeys = on === undefined ? rightOn : on;
  if (!predicate) validateKeys(left, right, leftKeys, rightKeys);

  const columns = [...new Set([...left.columns, ...right.columns])];
  const common = left.columns.filter((col) => right.columns.includes(col));
  const accessedLeft = new Set();
  const accessedRight = new Set();
  const leftProxies = predicate
    ? left.rows.map((row) => trackedRow(row, left.columns, accessedLeft))
    : [];
  const rightProxies = predicate
    ? right.rows.map((row) => trackedRow(row, right.columns, accessedRight))
    : [];
  const rightFirst = type === 'right' || type === 'rightAnti';
  const primary = rightFirst ? right : left;
  const secondary = rightFirst ? left : right;
  const primaryKeys = rightFirst ? rightKeys : leftKeys;
  const secondaryKeys = rightFirst ? leftKeys : rightKeys;
  const index = predicate ? undefined : indexRows(secondary, secondaryKeys);
  const isAnti = type === 'leftAnti' || type === 'rightAnti';
  const keepUnmatched = type !== 'inner';
  const matchedSecondary = new Set();
  const output = [];
  const unmatched = (row) =>
    Object.fromEntries(
      columns.map((col) => [col, Object.hasOwn(row, col) ? row[col] : null]),
    );

  primary.rows.forEach((row, i) => {
    const matches = predicate
      ? secondary.rows.filter((other, j) => {
          const l = rightFirst ? leftProxies[j] : leftProxies[i];
          const r = rightFirst ? rightProxies[i] : rightProxies[j];
          const matched = predicate(l, r);
          if (matched)
            rejectOverlap(
              common.filter((col) => !lodash.isEqual(row[col], other[col])),
            );
          return matched;
        })
      : lookup(index, row, primaryKeys);
    if (!matches.length && keepUnmatched) output.push(unmatched(row));
    for (const other of matches) {
      matchedSecondary.add(other);
      if (!isAnti)
        output.push(rightFirst ? { ...other, ...row } : { ...row, ...other });
    }
  });
  if (predicate && left.rows.length && right.rows.length) {
    rejectOverlap(
      common.filter((col) => !accessedLeft.has(col) || !accessedRight.has(col)),
    );
  }
  if (type === 'fullOuter') {
    for (const row of right.rows) {
      if (!matchedSecondary.has(row)) output.push(unmatched(row));
    }
  }
  return new DataFrame(output, columns);
}
