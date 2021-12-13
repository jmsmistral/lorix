import { group } from "d3-array";
import { v4 as uuid } from "uuid";

import { DataFrame } from "./dataframe.js";
import { _isSubsetArray } from "./utils.js";


export const unboundedPreceding = "UNBOUNDED_PRECEDING";
export const unboundedProceeding = "UNBOUNDED_PROCEEDING";
export const currentRow = "CURRENT_ROW";


function _generatePartitionFunctions(cols) {
    /**
     * Returns an array of functions (one for each col in `cols`) which
     * is used by d3-array to apply .rollup() and .group() (used in
     * calculating groupBy and window functions).
     */

    let partitionFunctions = [];
    for (let col of cols)
        partitionFunctions.push((g) => g[col]);
    return partitionFunctions;
}


function _precedingIndexThreshold(n, index) {
    /**
     * Limits the window size bound calculation
     * to the array size.
     */

    return ((index - n) < 0) ? 0 : (index - n);
}


function _procedingIndexThreshold(n, index, arrLength) {
    /**
     * Limits the window size bound calculation
     * to the array size.
     */

    return ((index + n) > (arrLength - 1)) ? (arrLength - 1) : (index + n) + 1;
}


function _getSliceIndices(partitionArray, currentIndex, prev, next) {
    /**
     * Calculate the index bounds defining the window size
     * for the given row within it's partition.
     */

    const pArrLength = partitionArray.length;

    if (prev == unboundedPreceding && next == unboundedProceeding)
        return [0, pArrLength - 1];

    if (prev == unboundedPreceding && next == currentRow)
        return [0, currentIndex + 1];

    if (prev == currentRow && next == unboundedProceeding)
        return [currentIndex];

    if (prev == currentRow && next == currentRow)
        return [currentIndex, currentIndex + 1];

    if (!isNaN(prev) && next == currentRow)
        return [_precedingIndexThreshold(prev, currentIndex), currentIndex + 1];

    if (prev == currentRow && !isNaN(next)) {
        return [currentIndex, _procedingIndexThreshold(next, currentIndex, pArrLength)];
    }

    if (prev == unboundedPreceding && !isNaN(next))
        return [0, _procedingIndexThreshold(next, currentIndex, pArrLength)];

    if (!isNaN(prev) && next == unboundedProceeding)
        return [_precedingIndexThreshold(prev, currentIndex)];

    if (!isNaN(prev) && !isNaN(next))
        return [_precedingIndexThreshold(prev, currentIndex), _procedingIndexThreshold(next, currentIndex, pArrLength)];

}


function _resizePartitionRows(arr, windowSize, windowArrayColName) {
    /**
     * Resizes an array `arr` as configured through `windowSize`, an
     * array of two values taking one of the following values:
     * - lorix.unboundedPreceding
     * - lorix.unboundedProceding
     * - lorix.currentRow
     * - Positive integer representing the number of rows previous to / following from the current row
     */

    // Get window size parameters
    const previous = windowSize[0];
    const following = windowSize[1];

    // Resize `arr`
    return arr.map( (row, index, partitionArray) => {
        row[windowArrayColName] = _getSliceIndices(partitionArray, index, previous, following);
        return row;
    });
}


function _runWindowOverMapWithWindowSize(partitions, partitionByCols, windowFunc, newCol, windowSize, resultAgg={}) {
    /**
     * Traverses the Map of partitioned rows, populating an output
     * object with both the partition columns, and the results of applying
     * the window function to the array of rows in the leaf nodes of the
     * Map.
     */

    // Generate temp unique id for col
    let windowArrayColName = uuid();
    let cleanRow, dummy;

    return Array.from(partitions, ([key, value]) => {
        if (value instanceof Map)
            return _runWindowOverMapWithWindowSize(value, partitionByCols.slice(1), windowFunc, newCol, windowSize, Object.assign({}, {...resultAgg, [partitionByCols[0]]: key}));

        // Add window indices to each row, and apply
        // window function to each adjusted sub-partition
        // per row.
        value = _resizePartitionRows(value, windowSize, windowArrayColName);
        return value.map( (r, i, v) => {
            ({[windowArrayColName]: dummy, ...cleanRow} = r);  // Remove temporary property holding window indices
            if (r[windowArrayColName].length == 1)
                return Object.assign({}, {...cleanRow, [newCol]: windowFunc(v.slice(r[windowArrayColName][0]), i)});
            return Object.assign({}, {...cleanRow, [newCol]: windowFunc(v.slice(r[windowArrayColName][0], r[windowArrayColName][1]), i)});
        });
    }).flat();
}


function _runWindowOverMap(partitions, partitionByCols, windowFunc, newCol, windowSize, resultAgg={}) {
    /**
     * Traverses the Map of partitioned rows, populating an output
     * object with both the partition columns, and the results of applying
     * the window function to the array of rows in the leaf nodes of the
     * Map.
     */

    return Array.from(partitions, ([key, value]) => {
        if (value instanceof Map)
            return _runWindowOverMap(value, partitionByCols.slice(1), windowFunc, newCol, windowSize, Object.assign({}, {...resultAgg, [partitionByCols[0]]: key}));
        return Object.assign({}, {...resultAgg, [partitionByCols[0]]: key, [newCol]: windowFunc(value)});
    });
}


export function window(windowFunc, partitionByCols=[], orderByCols=[], windowSize=[]) {
    /*
     * Used to enable calling window functions from .withColumn().
     * Also used to check and return the window parameters.
     */

    // Check window function parameters
    if (
        !(partitionByCols instanceof Array) ||
        !(orderByCols instanceof Array) ||
        !(windowSize instanceof Array) ||
        !(windowFunc instanceof Function) ||
        orderByCols.length > 2 ||
        windowSize.length > 2
    )
        throw Error("Invalid window function parameters defined. All parameters need to be provided.");

    // Return parameters as array (to enable destructuring in .withColumn())
    let windowParamProxy = () => [windowFunc, partitionByCols, orderByCols, windowSize];
    windowParamProxy.isWindow = true;  // flags as a window function for .withColumn to route correctly
    return windowParamProxy;
}


export function applyWindowFunction(df, newCol, windowFunc, partitionByCols, orderByCols, windowSize) {
    /**
     * @output
     * Retuns a DataFrame of the same size with the new column containing the
     * results of the window function.
     *
     * @input
     * - df: Dataframe on which the window function will be executed.
     * - windowFunc: Window function to be applied to each partition of rows.
     * - partitionByCols: Array of strings representing columns, used to partition the DataFrame rows.
     * - orderByCols: Array specifying the order columns and optionally another array specifying the order.
     * - windowSize: Array specifying the size of the window per partition, upon which the
     *               window function is applied.
     *
     * @example
     * Called using the .withColumn() instance method of a DataFrame instance:
     * - df.withColumn("newCol", lorix.window(lorix.sum("column"), ["colA"], [], [lorix.unboundedPreceding, lorix.currentRow]))
     */

    // Sort rows
    if (orderByCols.length == 1)
        df = df.orderBy([...partitionByCols , ...orderByCols[0]]);

    if (orderByCols.length == 2) {
        // Ensure that the groupBy columns are sorted in ascending order
        const partitionByOrder = partitionByCols.map((col) => "asc");
        df = df.orderBy([...partitionByCols , ...orderByCols[0]], [...partitionByOrder, ...orderByCols[1]]);
    }

    // Reset `windowSize` if full bounds are defined by end-user
    if (windowSize.length == 2 && (windowSize[0] == unboundedPreceding && windowSize[1] == unboundedProceeding))
        windowSize = [];

    if (windowFunc.hasOwnProperty("setWindowSize"))
        windowSize = windowFunc.windowSize;

    // Check that columns exist in Dataframe
    if (partitionByCols.length && !(_isSubsetArray(partitionByCols, df.columns)))
        throw Error(`Invalid columns provided in window function group or order array: '${partitionByCols}'`);

    if (orderByCols.length && !(_isSubsetArray(orderByCols[0], df.columns)))
        throw Error(`Invalid columns provided in window function group or order array: '${orderByCols[0]}'`);

    // Get Map with results per partition
    let partitionFunctions = _generatePartitionFunctions(partitionByCols);
    let partitionMap = group(df.rows, ...partitionFunctions);

    // Apply window function to ordered partitions
    if (partitionByCols.length) {
        if (windowSize.length)
            return new DataFrame(_runWindowOverMapWithWindowSize(partitionMap, partitionByCols, windowFunc, newCol, windowSize), df.columns.concat([newCol]));
        let windowDf = new DataFrame(_runWindowOverMap(partitionMap, partitionByCols, windowFunc, newCol, windowSize), partitionByCols.concat([newCol]));
        return df.leftJoin(windowDf, partitionByCols);
    }

    // If no partitioning is defined, apply window function across all rows
    const windowResult = windowFunc(partitionMap);
    return df.withColumn(newCol, () => windowResult);
}
