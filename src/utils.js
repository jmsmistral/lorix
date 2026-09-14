import lodash from 'lodash';

export function _isSubsetArray(arr, compareArray) {
  if (!Array.isArray(arr) || !Array.isArray(compareArray))
    throw Error('Array expected.');
  return arr.length > 0 && arr.every((value) => compareArray.includes(value));
}

export function _isString(value) {
  return Object.prototype.toString.call(value) === '[object String]';
}

export function _isValidColumnName(col) {
  return _isString(col) && col.length > 0 && /^[a-zA-Z_]/.test(col);
}

export function _getUniqueObjectProperties(rows) {
  if (!Array.isArray(rows)) throw Error('Expected an array of objects.');
  if (!rows.length) return [];
  const isRow = (row) =>
    row !== null && typeof row === 'object' && !Array.isArray(row);
  if (!rows.every(isRow)) throw Error('DataFrame rows must be objects.');
  const columns = Object.keys(rows[0]);
  const sorted = [...columns].sort();
  if (!rows.every((row) => lodash.isEqual(Object.keys(row).sort(), sorted))) {
    throw Error('DataFrame columns must be equal across rows.');
  }
  return columns;
}

// A nested Map preserves tuple boundaries and scalar types without serialization.
// Object-valued cells use reference identity; NaN values compare equal.
export function _distinctRows(rows, columns) {
  const seen = new Map();
  const end = Symbol('tuple end');
  return rows.filter((row) => {
    let node = seen;
    for (const col of columns) {
      const value = row[col];
      if (!node.has(value)) node.set(value, new Map());
      node = node.get(value);
    }
    if (node.has(end)) return false;
    node.set(end, true);
    return true;
  });
}

export function* _groupLeaves(groups) {
  if (groups instanceof Map) {
    for (const value of groups.values()) yield* _groupLeaves(value);
  } else {
    yield groups;
  }
}
