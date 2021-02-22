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

function _flattenAggMap(tree, level, groupByCols, colName) {
    // const m = new Map();
    // let o = {};
    for (const [key, value] of tree) {
        if (value instanceof Map) {
            return _flattenAggMap(value, level + 1, groupByCols, colName);
            // for (const [key2, value2] of _flattenAggMap(value)) {
            //     m.set(`${key}${sep}${key2}`, value2);
            // }
        } else {
            // m.set(key, value);
            o[key] = value;
        }
    }
    return m;
}


export function groupByAggregation(df, groupByCols, groupByAggs) {
    // Retuns either
    // Check that columns exist in Dataframe
    if (!(_isColumnArrayInDataframe(df.columns, groupByCols))) {
        throw Error(`Invalid columns provided to groupBy '${groupByCols}'`);
    }

    // If no aggregation map is passed, return the group Map
    let groupByFunctions = _generateGroupByFunctions(groupByCols);
    if ((arguments.length == 2) && (groupByAggs == undefined)) {
        return d3Array.group(df.rows, ...groupByFunctions);
    }

    console.log(groupByAggs);
    let dfs = [];
    for (let [k, v] of Object.entries(groupByAggs)) {
        console.log(k, v);
        if (v == "sum") {
            console.log(d3Array.rollup(df.rows, v => d3Array.sum(v, d => d[k]), ...groupByFunctions));
        }

    }
}




// export class GroupBy {
//     constructor(df, columns) {
//         this.parentDataframe = df;
//         this.groupByColumns = this._validateGroupByColumns(df.columns, columns);
//         this.groupByFunctions = this._generateGroupByFunctions();
//         this.groups = this._generateGroups();
//         this.numGroups = this.groupByColumns.length;
//         // console.log(this.numGroups);
//         // console.log(this.groups);
//         // console.log(this.groups.size);
//         console.log(this.groups.keys());
//         console.log(this.groups.get(1).keys());
//     }

//     _validateGroupByColumns(dfCols, groupByCols) {
//         if (_isColumnArrayInDataframe(dfCols, groupByCols)) {
//             return groupByCols;
//         }
//         throw Error("Invalid columns provided in groupBy");
//     }

//     _generateGroupByFunctions() {
//         let groupByFunctions = [];
//         for (let col of this.groupByColumns)
//             groupByFunctions.push((g) => g[col]);
//         return groupByFunctions;
//     }

//     _generateGroups() {
//         return d3Array.group(this.parentDataframe.rows, ...this.groupByFunctions);
//     }

//     agg(aggregations) {
//         // Returns a Dataframe with the aggregations performed
//         // on `this.groups` based on the definition defined
//         // in the `aggregations` map. The available aggregations
//         // are "sum", "count", "mean", "median"
//         // e.g. aggregations = {
//         //     "colA": "sum",
//         //     "colB": "count"
//         // }
//     }

// }
