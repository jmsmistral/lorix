import d3Array from 'd3-array';
import lodash from 'lodash';

import { _isColumnArrayInDataframe } from './utils.js';


export class GroupBy {
    constructor(df, columns) {
        this.parentDataframe = df;
        this.groupByColumns = this._validateGroupByColumns(df.columns, columns);
        this.groupByFunctions = this._generateGroupByFunctions();
        this.groups = this._generateGroups();
        this.numGroups = this.groupByColumns.length;
        // console.log(this.numGroups);
        // console.log(this.groups);
        // console.log(this.groups.size);
        console.log(this.groups.keys());
        console.log(this.groups.get(1).keys());
    }

    _validateGroupByColumns(dfCols, groupByCols) {
        if (_isColumnArrayInDataframe(dfCols, groupByCols)) {
            return groupByCols;
        }
        throw Error("Invalid columns provided in groupBy");
    }

    _generateGroupByFunctions() {
        let groupByFunctions = [];
        for (let col of this.groupByColumns)
            groupByFunctions.push((g) => g[col]);
        return groupByFunctions;
    }

    _generateGroups() {
        return d3Array.group(this.parentDataframe.rows, ...this.groupByFunctions);
    }

}
