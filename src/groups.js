import d3Array from 'd3-array';
import lodash from 'lodash';

import { DataFrame } from "./dataframe.js";
import { _isSubsetArray } from './utils.js';


// GROUPBY FUNCTIONS
function _generateGroupByFunctions(cols) {
    let groupByFunctions = [];
    for (let col of cols)
        groupByFunctions.push((g) => g[col]);
    return groupByFunctions;
}


function _flattenAggMap(groups, groupByCols, aggColName, p = {}) {
    return Array.from(groups, ([key, value]) =>
        value instanceof Map
        ? _flattenAggMap(value, groupByCols.slice(1), aggColName, Object.assign({}, { ...p, [groupByCols[0]]: key } ))
        : Object.assign({}, { ...p, [groupByCols[0]]: key, [aggColName] : value })
    ).flat();
}

function _aggregate(type, df, groupByFunctions, groupByCols, aggCol) {
    // let map = undefined;
    let map;
    let aggColumnName = aggCol + "_" + type;
    if (type == "sum") {
        map = d3Array.rollup(df.rows, v => d3Array.sum(v, d => d[aggCol]), ...groupByFunctions);
    }
    if (type == "mean") {
        map = d3Array.rollup(df.rows, v => d3Array.mean(v, d => d[aggCol]), ...groupByFunctions);
    }
    if (type == "min") {
        map = d3Array.rollup(df.rows, v => d3Array.min(v, d => d[aggCol]), ...groupByFunctions);
    }
    if (type == "max") {
        map = d3Array.rollup(df.rows, v => d3Array.max(v, d => d[aggCol]), ...groupByFunctions);
    }
    if (type == "count") {
        map = d3Array.rollup(df.rows, v => v.length, ...groupByFunctions);
    }

    // Catch any undefined aggregation types passed
    if (map == undefined) {
        throw Error(`Invalid aggregation provided to groupBy '${type}'`);
    }

    return new DataFrame(_flattenAggMap(map, groupByCols, aggColumnName), groupByCols.concat([aggColumnName]));
}


export function groupAggregation(df, groupByCols, groupByAggs) {
    /**
     * @output
     * Retuns either:
     * - A new Dataframe object with the results of the aggregations
     *   defined in `groupByAggs` for each distinct group of the list
     *   of columns in `groupByCols`.
     * - A nested Map object, if no aggregation is provided through
     *   `groupByAggs`, with the values of each `groupByCols` column
     *   as the keys at each level of the Map.
     *
     * @input
     * - df: Dataframe on which the groupBy will be executed.
     * - groupByCols: Array of strings representing columns to group by.
     * - groupByAggs: Object of column-to-aggregation(s) mappings that define
     * the aggregation(s) to perform for each column, across each group.
     * A column can have one or more aggregations defined, and this is
     * passed as either a string or an array of strings specifying the
     * aggregations. e.g. {"colA": "sum", "colB": ["sum", "count"]}.
     */

    // If no aggregation object is passed, return a Map defining the groups
    let groupByFunctions = _generateGroupByFunctions(groupByCols);
    if (groupByAggs == undefined) {
        return d3Array.group(df.rows, ...groupByFunctions);
    }

    // Check that columns exist in Dataframe
    let aggCols = Object.getOwnPropertyNames(groupByAggs);
    if (!(_isSubsetArray(groupByCols.concat(aggCols), df.columns))) {
        throw Error(`Invalid columns provided to groupBy '${groupByCols.concat(aggCols)}'`);
    }

    let dfs = [];
    for (let [k, v] of Object.entries(groupByAggs)) {
        // Check if value is list of aggregations for the
        // given column, or a single one.
        if (v instanceof Array) {
            for (let agg of v) {
                dfs.push(_aggregate(agg, df, groupByFunctions, groupByCols, k));
            }
        } else {
            dfs.push(_aggregate(v, df, groupByFunctions, groupByCols, k));
        }
    }

    // Join resultant Dataframes in dfs
    if (dfs.length > 1) {
        return dfs.reduce((df1, df2) => df1.innerJoin(df2, groupByCols));
    }

    return dfs[0];
}



// WINDOW FUNCTIONS
export function min(col) {
    return (v => d3Array.min(v, d => d[col]));
}

export function max(col) {
    return (v => d3Array.max(v, d => d[col]));
}

export function median(col) {
    return (v => d3Array.median(v, d => d[col]));
}

export function quantile(col, p = 0.5) {
    return (v => d3Array.quantile(v, p, d => d[col]));
}

export function variance(col) {
    return (v => d3Array.variance(v, d => d[col]));
}

export function stdev(col) {
    return (v => d3Array.deviation(v, d => d[col]));
}

function _windowFlattenAggMap(groups, groupByCols, aggColName, p = {}) {
    return Array.from(groups, ([key, value]) =>
        value instanceof Map
        ? _windowFlattenAggMap(value, groupByCols.slice(1), aggColName, Object.assign({}, { ...p, [groupByCols[0]]: key } ))
        : Object.assign({}, { ...p, [groupByCols[0]]: key, [aggColName] : value })
    ).flat();
}

function _windowAggregate(aggFunc, df, groupByFunctions, groupByCols, aggCol) {
    let map = d3Array.rollup(df.rows, aggFunc, ...groupByFunctions);
    return new DataFrame(_windowFlattenAggMap(map, groupByCols, aggCol), groupByCols.concat([aggCol]));
}

export function groupSortAggregation(df, groupByCols, orderByCols, groupByAggs) {
    /**
     * @output
     * Retuns either:
     * - A new Dataframe object with the results of the aggregations
     *   defined in `groupByAggs` for each distinct group of the list
     *   of columns in `groupByCols`.
     * - A nested Map object, if no aggregation is provided through
     *   `groupByAggs`, with the values of each `groupByCols` column
     *   as the keys at each level of the Map.
     *
     * @input
     * - df: Dataframe on which the groupBy will be executed.
     * - groupByCols: Array of strings representing columns to group by.
     * - orderByCols: Array specifying the order columns and optionally another array specifying the order.
     * - groupByAggs: Object of column-to-aggregation(s) mappings that define
     * the aggregation(s) to perform for each column, across each group.
     * A column can have one or more aggregations defined, and this is
     * passed as either a string or an array of strings specifying the
     * aggregations. e.g. {"colA": "sum", "colB": ["sum", "count"]}.
     *
     * @example
     */

    // console.log(arguments.length);
    // console.log(df);
    // console.log(groupByCols);
    // console.log(orderByCols);
    // console.log(groupByAggs);
    // console.log(groupByAggs instanceof Array);
    // console.log(orderByCols instanceof Array);

    // Inputs check
    if (
        !(groupByCols instanceof Array) ||
        !(orderByCols instanceof Array) ||
        groupByCols.length < 1 ||
        orderByCols.length < 1 ||
        orderByCols.length > 2
    ) {
        throw Error("Invalid groupBy or orderBy columns provided. Both parameters need to be supplied.");
    }

    // Sort rows
    if (orderByCols.length == 1) {
        df = df.orderBy([...groupByCols , ...orderByCols[0]]);
    }
    if (orderByCols.length == 2) {
        // Ensure that the groupBy columns are sorted in ascending order
        const groupByOrder = groupByCols.map((col) => "asc");
        df = df.orderBy([...groupByCols , ...orderByCols[0]], [...groupByOrder, ...orderByCols[1]]);
    }

    // If no aggregation map is passed, return the group Map
    let groupByFunctions = _generateGroupByFunctions(groupByCols);
    if (groupByAggs == undefined) {
        return d3Array.group(df.rows, ...groupByFunctions);
    }

    // Check that columns exist in Dataframe
    if (!(_isSubsetArray([...groupByCols, ...orderByCols[0]], df.columns))) {
        throw Error(`Invalid columns provided in group or order array '${[...groupByCols, ...orderByCols[0]]}'`);
    }

    let dfs = [];
    for (let [k, v] of Object.entries(groupByAggs)) {
        // Check if value is list of aggregations for the
        // given column, or a single one.
        if (v instanceof Array) {
            for (let agg of v) {
                if (agg instanceof Function) {
                    dfs.push(_windowAggregate(agg, df, groupByFunctions, groupByCols, k));
                }
            }
        } else {
            if (v instanceof Function) {
                dfs.push(_windowAggregate(v, df, groupByFunctions, groupByCols, k));
            }
        }
    }

    // Join resultant Dataframes in dfs
    if (dfs.length > 1) {
        const windowAggResults = dfs.reduce((df1, df2) => df1.innerJoin(df2, groupByCols));
        return df.leftJoin(windowAggResults, groupByCols);
    }

    return dfs[0];
}
