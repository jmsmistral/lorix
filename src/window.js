import d3Array from 'd3-array';

import { DataFrame } from "./dataframe.js";
import { _isSubsetArray } from './utils.js';


export const unboundedPreceding = 0;
export const unboundedProceding = Infinity;
export const currentRow = 1;


export function min(col) {
    return (v => d3Array.min(v, d => d[col]));
}

export function max(col) {
    return (v => d3Array.max(v, d => d[col]));
}

export function median(col) {
    return (v => d3Array.median(v, d => d[col]));
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


function _runWindowOverMap(groups, partitionByCols, windowFunc, newCol, resultAgg={}) {
    /**
     * Traverses the Map of partitioned rows, populating an output
     * object with both the partition columns, and the results of applying
     * the window function to the array of rows in the leaf nodes of the
     * Map.
     */

    return Array.from(groups, ([key, value]) => {
        if (value instanceof Map)
            return _runWindowOverMap(value, partitionByCols.slice(1), windowFunc, newCol, Object.assign({}, {...resultAgg, [partitionByCols[0]]: key} ));
        return Object.assign({}, {...resultAgg, [partitionByCols[0]]: key, [newCol]: windowFunc(value)});
    });
}


export function window(windowFunc, partitionByCols, orderByCols=[], windowSize=[]) {
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
        partitionByCols.length < 1 ||
        orderByCols.length < 1 ||
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
     * - windowFunc: Window function to be applied to each group of rows.
     * - partitionByCols: Array of strings representing columns, used to partition the DataFrame rows.
     * - orderByCols: Array specifying the order columns and optionally another array specifying the order.
     * - windowSize: Array specifying the size of the window per group, upon which the
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
        const groupByOrder = partitionByCols.map((col) => "asc");
        df = df.orderBy([...partitionByCols , ...orderByCols[0]], [...groupByOrder, ...orderByCols[1]]);
    }

    // Check that columns exist in Dataframe
    if (!(_isSubsetArray([...partitionByCols, ...orderByCols[0]], df.columns)))
        throw Error(`Invalid columns provided in window function group or order array: '${[...partitionByCols, ...orderByCols[0]]}'`);

    // Get Map with results per group
    let partitionFunctions = _generatePartitionFunctions(partitionByCols);
    let map = d3Array.group(df.rows, ...partitionFunctions);

    // Apply window function to ordered partitions
    let windowDf = new DataFrame(_runWindowOverMap(map, partitionByCols, windowFunc, newCol), partitionByCols.concat([newCol]));

    return df.leftJoin(windowDf, partitionByCols);
}
