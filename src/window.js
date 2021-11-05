import d3Array from 'd3-array';
import lodash from 'lodash';
import { v4 as uuid } from 'uuid';

import { DataFrame } from "./dataframe.js";
import { _isSubsetArray } from './utils.js';


export const unboundedPreceding = "UNBOUNDED_PRECEDING";
export const unboundedProceding = "UNBOUNDED_PROCEDING";
export const currentRow = "CURRENT_ROW";


export function rowNumber() {
    return (v => v.keys());
}

export function sum(col) {
    return (v => d3Array.sum(v, d => d[col]));
}

export function min(col) {
    return (v => d3Array.min(v, d => d[col]));
}

export function max(col) {
    return (v => d3Array.max(v, d => d[col]));
}

export function mean(col) {
    return (v => d3Array.mean(v, d => d[col]));
}

export function median(col) {
    return (v => d3Array.median(v, d => d[col]));
}

export function mode(col) {
    return (v => d3Array.mode(v, d => d[col]));
}

export function quantile(col, p=0.5) {
    return (v => d3Array.quantile(v, p, d => d[col]));
}

export function variance(col) {
    return (v => d3Array.variance(v, d => d[col]));
}

export function stdev(col) {
    return (v => d3Array.deviation(v, d => d[col]));
}


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
    return ((index - n) < 0) ? 0 : (index - n);
}


function _procedingIndexThreshold(n, index, arrLength) {
    return ((index + n) > (arrLength - 1)) ? (arrLength - 1) : (index + n);
}


function _getSliceIndices(partitionArray, currentIndex, prev, next) {
    const pArrLength = partitionArray.length;

    if (prev == unboundedPreceding && next == unboundedProceding)
            return;
            // return partitionArray;

    if (prev == unboundedPreceding && next == currentRow)
        return [0, currentIndex + 1];
        // return partitionArray.slice(0, currentIndex + 1);

    if (prev == currentRow && next == unboundedProceding)
        return [currentIndex];
        // return partitionArray.slice(currentIndex);

    if (prev == currentRow && next == currentRow)
        return [currentIndex, currentIndex + 1];
        // return partitionArray.slice(currentIndex, currentIndex + 1);

    if (!isNaN(prev) && next == currentRow)
        return [_precedingIndexThreshold(prev, currentIndex), currentIndex + 1];
        // return partitionArray.slice(_precedingIndexThreshold(prev, currentIndex), currentIndex + 1);

    if (prev == currentRow && !isNaN(next))
        return [currentIndex, _procedingIndexThreshold(next, currentIndex, pArrLength)];
        // return partitionArray.slice(currentIndex, _procedingIndexThreshold(next, currentIndex, pArrLength));

    if (prev == unboundedPreceding && !isNaN(next))
        return [0, _procedingIndexThreshold(next, currentIndex, pArrLength)];
        // return partitionArray.slice(0, _procedingIndexThreshold(next, currentIndex, pArrLength));

    if (!isNaN(prev) && next == unboundedProceding)
        return [_precedingIndexThreshold(prev, currentIndex)];
        // return partitionArray.slice(_precedingIndexThreshold(prev, currentIndex));

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


function _runWindowOverMapWithWindowSize(groups, partitionByCols, windowFunc, newCol, windowSize, resultAgg={}) {

    // Generate temp unique id for col
    const windowArrayColName = "WINDOW_RESULT_COL_" + uuid();
    console.log(windowArrayColName);





    if (windowSize.length) {
        // console.log(value);
        value = _resizePartitionRows(value, windowSize, windowArrayColName);
        console.log("window size calculated:");
        console.log(value);
        let result = value.map( (r, i, v) => {
            if (r[windowArrayColName].length == 1)
                return windowFunc(v.slice(r[windowArrayColName[0]]));
            return windowFunc(v.slice(r[windowArrayColName[0], windowArrayColName[1]]));
        });
        console.log("result:");
        console.log(result);
        // for (const row of result)
        //     console.log(row);
        console.log();
        // let windowRows = []
        // for (let row of value) {
        //     console.log("row:");
        //     console.log(row);
        //     console.log();
            // console.log(row[windowArrayColName]);
            // console.log(windowFunc(row[windowArrayColName]));
            // windowRows.push(Object.assign({}, {...resultAgg, [partitionByCols[0]]: key, [newCol]: windowFunc(row[windowArrayColName])}));
        // }
        // return windowRows;
    }
}


function _runWindowOverMap(groups, partitionByCols, windowFunc, newCol, windowSize, resultAgg={}) {
    /**
     * Traverses the Map of partitioned rows, populating an output
     * object with both the partition columns, and the results of applying
     * the window function to the array of rows in the leaf nodes of the
     * Map.
     */

    return Array.from(groups, ([key, value]) => {
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

    // Check that columns exist in Dataframe
    if (partitionByCols.length && !(_isSubsetArray(partitionByCols, df.columns)))
        throw Error(`Invalid columns provided in window function group or order array: '${partitionByCols}'`);

    if (orderByCols.length && !(_isSubsetArray(orderByCols[0], df.columns)))
        throw Error(`Invalid columns provided in window function group or order array: '${orderByCols[0]}'`);

    // Get Map with results per group
    let partitionFunctions = _generatePartitionFunctions(partitionByCols);
    let map = d3Array.group(df.rows, ...partitionFunctions);

    // Apply window function to ordered partitions
    if (partitionByCols.length) {
        let windowDf = new DataFrame(_runWindowOverMap(map, partitionByCols, windowFunc, newCol, windowSize), partitionByCols.concat([newCol]));
        return df.leftJoin(windowDf, partitionByCols);
    }

    // If no partitioning is defined, apply window function across all rows
    const windowResult = windowFunc(map);
    return df.withColumn(newCol, () => windowResult);
}
