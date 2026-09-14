import { group } from 'd3-array';
import { DataFrame } from './dataframe.js';
import { _groupLeaves } from './utils.js';

export const unboundedPreceding = 'UNBOUNDED_PRECEDING';
export const unboundedProceeding = 'UNBOUNDED_PROCEEDING';
export const currentRow = 'CURRENT_ROW';

function validateFrame(bounds) {
  const valid = (value, unbounded) =>
    value === currentRow ||
    value === unbounded ||
    (Number.isSafeInteger(value) && value >= 0);
  if (
    !Array.isArray(bounds) ||
    (bounds.length !== 0 &&
      (bounds.length !== 2 ||
        !valid(bounds[0], unboundedPreceding) ||
        !valid(bounds[1], unboundedProceeding)))
  ) {
    throw Error(
      'Window bounds must be empty or a preceding/following pair of non-negative integers or window constants.',
    );
  }
}

export function window(
  windowFunc,
  partitionByCols = [],
  orderByCols = [],
  windowSize = [],
) {
  if (
    typeof windowFunc !== 'function' ||
    !Array.isArray(partitionByCols) ||
    !Array.isArray(orderByCols) ||
    orderByCols.length > 2 ||
    orderByCols.some((cols) => !Array.isArray(cols) || !cols.length)
  ) {
    throw Error('Invalid window function parameters.');
  }
  validateFrame(windowSize);
  const descriptor = () => [
    windowFunc,
    partitionByCols,
    orderByCols,
    windowSize,
  ];
  descriptor.isWindow = true;
  return descriptor;
}

export function applyWindowFunction(
  df,
  newCol,
  windowFunc,
  partitionByCols,
  orderByCols,
  windowSize,
) {
  const valueCol = windowFunc.columnPropName;
  for (const col of [
    ...partitionByCols,
    ...(orderByCols[0] || []),
    ...(valueCol === undefined ? [] : [valueCol]),
  ]) {
    if (!df.columns.includes(col))
      throw Error(`Invalid column in window function: '${col}'.`);
  }
  if (orderByCols.length) {
    const cols = [...partitionByCols, ...orderByCols[0]];
    const orders = [
      ...partitionByCols.map(() => 'asc'),
      ...(orderByCols[1] || orderByCols[0].map(() => 'asc')),
    ];
    df = df.orderBy(cols, orders);
  }
  const bounds = windowFunc.setWindowSize ? windowFunc.windowSize : windowSize;
  validateFrame(bounds);
  // Group indexed entries so repeated row references and interleaved groups
  // retain their positions. Neither the source nor callback rows get metadata.
  const entries = df.rows.map((row, index) => ({ row, index }));
  const partitions = group(
    entries,
    ...partitionByCols.map((col) => (entry) => entry.row[col]),
  );
  const output = new Array(df.rows.length);
  for (const partition of _groupLeaves(partitions)) {
    const rows = partition.map((entry) => entry.row);
    const wholePartition =
      !windowFunc.setWindowSize &&
      (!bounds.length ||
        (bounds[0] === unboundedPreceding &&
          bounds[1] === unboundedProceeding));
    const aggregate = wholePartition ? windowFunc(rows) : undefined;
    partition.forEach((entry, index) => {
      let result = aggregate;
      if (!wholePartition) {
        const [before, after] = bounds;
        const start =
          before === unboundedPreceding
            ? 0
            : Math.max(0, index - (before === currentRow ? 0 : before));
        const end =
          after === unboundedProceeding
            ? rows.length
            : Math.min(
                rows.length,
                index + (after === currentRow ? 0 : after) + 1,
              );
        result = windowFunc(rows.slice(start, end), index);
      }
      output[entry.index] = { ...entry.row, [newCol]: result };
    });
  }
  return new DataFrame(
    output,
    df.columns.includes(newCol) ? df.columns : [...df.columns, newCol],
  );
}
