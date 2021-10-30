import d3Array from 'd3-array';

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

function _generateGroupByFunctions(cols) {
    let groupByFunctions = [];
    for (let col of cols)
        groupByFunctions.push((g) => g[col]);
    return groupByFunctions;
}


function _windowFlattenAggMap(groups, groupByCols, windowFunc, p={}) {
    // console.log(groups);
    // console.log(groupByCols);
    // console.log(windowFunc.toString());

    let result = Array.from(groups, ([key, value]) => {
        if (value instanceof Map) {
            return _windowFlattenAggMap(value, groupByCols.slice(1), windowFunc, Object.assign({}, { ...p, [groupByCols[0]]: key } ));
        } else {
            console.log(value[0]);
            // console.log(windowFunc(value[0]))
            // Object.assign({}, { ...p, [groupByCols[0]]: key, "result": windowFunc(value) })
            // Object.assign({}, { ...p, [groupByCols[0]]: key, [aggColName]: windowFunc(value) })
            // console.log(value);
        }
    }); //.flat();

    console.log(result);
}


function _windowAggregate(windowFunc, df, groupByFunctions, groupByCols, aggCol) {

    let map = d3Array.group(df.rows, ...groupByFunctions);
    // let map = d3Array.rollup(df.rows, windowFunc, ...groupByFunctions);
    // return new DataFrame(_windowFlattenAggMap(map, groupByCols, aggCol), groupByCols.concat([aggCol]));
}

export function window(windowFunc, groupByCols, orderByCols=[], windowSize=[]) {
    /*
     * Used to enable calling window functions from .withColumn().
     * Also used to check and return the window parameters.
     */

    // Check window function parameters
    if (
        !(groupByCols instanceof Array) ||
        !(orderByCols instanceof Array) ||
        !(windowSize instanceof Array) ||
        !(windowFunc instanceof Function) ||
        groupByCols.length < 1 ||
        orderByCols.length < 1 ||
        orderByCols.length > 2 ||
        windowSize.length > 2
    )
        throw Error("Invalid window function parameters defined. All parameters need to be provided.");

    // Return parameters as array (to enable destructuring in .withColumn())
    let windowParamProxy = () => [windowFunc, groupByCols, orderByCols, windowSize];
    windowParamProxy.isWindow = true;  // flags as a window function for .withColumn to route correctly
    return windowParamProxy;
}


export function applyWindowFunction(df, windowFunc, groupByCols, orderByCols, windowSize) {
    /**
     * @output
     * Retuns a DataFrame of the same size with the new column containing the
     * results of the window function.
     *
     * @input
     * - df: Dataframe on which the window function will be executed.
     * - windowFunc: Window function to be applied to each group of rows.
     * - groupByCols: Array of strings representing columns to group by.
     * - orderByCols: Array specifying the order columns and optionally another array specifying the order.
     * - windowSize: Array specifying the size of the window per group, upon which the
     *               window function is applied.
     *
     * @example
     * Called using the .withColumn() instance method of a DataFrame instance:
     * - df.withColumn("newCol", lorix.window(lorix.sum("column"), ["colA"], [], [lorix.unboundedPreceding, lorix.currentRow]))
     */

    console.log("arguments.length: " + arguments.length);
    console.log("df: " + df);
    console.log("windowFunc: " + windowFunc);
    console.log("groupByCols: " + groupByCols);
    console.log("orderByCols: " + orderByCols);
    console.log("windowSize: " + windowSize);
    console.log(groupByCols instanceof Array);
    console.log(orderByCols instanceof Array);
    console.log(windowSize instanceof Array);

    // Sort rows
    if (orderByCols.length == 1)
        df = df.orderBy([...groupByCols , ...orderByCols[0]]);

    if (orderByCols.length == 2) {
        // Ensure that the groupBy columns are sorted in ascending order
        const groupByOrder = groupByCols.map((col) => "asc");
        df = df.orderBy([...groupByCols , ...orderByCols[0]], [...groupByOrder, ...orderByCols[1]]);
    }

    // Check that columns exist in Dataframe
    if (!(_isSubsetArray([...groupByCols, ...orderByCols[0]], df.columns)))
        throw Error(`Invalid columns provided in window function group or order array: '${[...groupByCols, ...orderByCols[0]]}'`);

    // Get Map with results per group
    let groupByFunctions = _generateGroupByFunctions(groupByCols);
    let map = d3Array.group(df.rows, ...groupByFunctions);
    console.log(map);

    // Apply window function to ordered partitions
    _windowFlattenAggMap(map, groupByCols, windowFunc);

    // return new DataFrame(_windowFlattenAggMap(map, groupByCols, aggCol), groupByCols.concat([aggCol]));

    // let dfs = [];
    // for (let [k, v] of Object.entries(groupByAggs)) {
    //     // Check if value is list of aggregations for the
    //     // given column, or a single one.
    //     if (v instanceof Array) {
    //         for (let agg of v) {
    //             if (agg instanceof Function) {
    //                 dfs.push(_windowAggregate(agg, df, groupByFunctions, groupByCols, k));
    //             }
    //         }
    //     } else {
    //         if (v instanceof Function) {
    //             dfs.push(_windowAggregate(v, df, groupByFunctions, groupByCols, k));
    //         }
    //     }
    // }

    // Join resultant Dataframes in dfs
    // if (dfs.length > 1) {
    //     const windowAggResults = dfs.reduce((df1, df2) => df1.innerJoin(df2, groupByCols));
    //     return df.leftJoin(windowAggResults, groupByCols);
    // }

    // return dfs[0];
}
