import d3Array from 'd3-array';
import lodash from 'lodash';

import { DataFrame } from "./dataframe.js";
import { _isColumnArrayInDataframe } from './utils.js';


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
    let map;
    let aggColumnName = aggCol + "_" + type;
    if (type == "sum") {
        map = d3Array.rollup(df.rows, v => d3Array.sum(v, d => d[aggCol]), ...groupByFunctions);
    }
    if (type == "mean") {
        map = d3Array.rollup(df.rows, v => d3Array.mean(v, d => d[aggCol]), ...groupByFunctions);
    }
    if (type == "count") {
        map = d3Array.rollup(df.rows, v => v.length, ...groupByFunctions);
    }

    return new DataFrame(_flattenAggMap(map, groupByCols, aggColumnName), groupByCols.concat([aggColumnName]));
}


export function groupByAggregation(df, groupByCols, groupByAggs) {
    /**
     * @output:
     * Retuns either:
     * - A new Dataframe object with the results of the aggregations
     *   defined in `groupByAggs` for each distinct group of the list
     *   of columns in `groupByCols`.
     * - A nested Map object, if no aggregation is provided through
     *   `groupByAggs`, with the values of each `groupByCols` column
     *   as the keys at each level of the Map.
     *
     * @input:
     * - df: Dataframe on which the groupBy will be executed.
     * - groupByCols: Array of strings representing columns to group by.
     * - groupByAggs: Object of column-to-aggregation(s) mappings that define
     * the aggregation(s) to perform for each column, across each group.
     * A column can have one or more aggregations defined, and this is
     * passed as either a string or an array of strings specifying the
     * aggregations. e.g. {"colA": "sum", "colB": ["sum", "count"]}.
     */

    // Check that columns exist in Dataframe
    let aggCols = Object.getOwnPropertyNames(groupByAggs);
    if (!(_isColumnArrayInDataframe(df.columns, groupByCols.concat(aggCols)))) {
        throw Error(`Invalid columns provided to groupBy '${groupByCols.concat(aggCols)}'`);
    }

    // If no aggregation map is passed, return the group Map
    let groupByFunctions = _generateGroupByFunctions(groupByCols);
    if ((arguments.length == 2) && (groupByAggs == undefined)) {
        return d3Array.group(df.rows, ...groupByFunctions);
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
