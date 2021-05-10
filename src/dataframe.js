import lodash from "lodash";

import {
    _crossJoin,
    _join
} from "./joins.js";

import {
    groupAggregation,
    groupSortAggregation
} from "./groups.js"

import {
    validateFunctionReferencesWithProxy,
    _getUniqueObjectProperties,
    _isValidColumnName
} from "./utils.js";



export class DataFrame {
    constructor(rowArray=[], columns=[]) {
        this.rows = rowArray;
        this.columns = columns;
    }

    // Enable to iterate over DataFrame rows
    *iterator() {
        for (let row of this.rows) {
            yield row;
        }
    }

    [Symbol.iterator]() {
        return this.iterator();
    }

    static fromArray(arr) {
        // Return a Dataframe from an array of objects.
        if (arr instanceof Array) {
            let cols = _getUniqueObjectProperties(arr);
            if (cols.length > 0) {
                return new DataFrame(arr, cols);
            }
        }
        throw Error("Dataframe.fromArray() only accepts a non-empty array of objects.");
    }

    toArray() {
        return this.rows;
    }

    head(n=10) {
        if (n > 0) {
            console.table(this.rows.slice(0, n), this.columns);
        }
    }

    slice(i=0, j=-1) {
        // Returns a DataFrame with the subset of rows
        // specified by the i, j indexes.
        if (j !== -1)
            return new DataFrame(this.rows.slice(i, j + 1), this.columns);
        return new DataFrame(this.rows.slice(i), this.columns);
    }

    select(...fields) {
        if (!fields.length) {
            throw Error("No columns provided to select().");
        }
        // Check that fields passed exist in DataFrame
        const diff = lodash.difference(fields, this.columns)
        if (diff.length) {
            throw Error(`Field(s) '${diff.join(', ')}' do not exist in DataFrame.`);
        }
        const outputArray = [];
        for (const row of this.rows) {
            outputArray.push(lodash.pick(row, fields))
        }
        return new DataFrame(outputArray, fields);
    }

    drop(...fields) {
        if (!fields.length) {
            throw Error("No columns provided to drop().");
        }
        // Check that fields passed exist in DataFrame
        const diff = lodash.difference(fields, this.columns)
        if (diff.length) {
            throw Error(`Field(s) '${diff.join(', ')}' do not exist in DataFrame.`);
        }
        const outputArray = [];
        for (const row of this.rows) {
            outputArray.push(lodash.omit(row, fields))
        }
        return new DataFrame(outputArray, lodash.difference(this.columns, fields));
    }

    withColumn(col, expr) {
        // Returns a new Dataframe with a new column definition.
        // Note: if a reference is made to a non-existent column
        // the result will be undefined.
        // Check that `col` is a string that does not start with a number.
        if (!_isValidColumnName(col)) {
            throw Error(`Column name "${col}" is not valid.`);
        }

        // Check that `expr` is a function.
        if(expr & !(expr instanceof Function)) {
            throw Error("expr provided to withColumn needs to be a function.")
        }

        // Check what existing columns are being referenced,
        // if any, and throw an error if at least one does not exist.
        validateFunctionReferencesWithProxy(expr, this.columns);

        let newRows = this.rows.map((row) => ({...row, ...{[col]: expr(row)}}));
        return new DataFrame(newRows, this.columns.concat([col]));
    }

    crossJoin(df) {
        if (arguments.length < 1 || arguments.length > 1) {
            throw Error(`crossJoin takes a single argument. Arguments passed: ${arguments.length}`);
        }
        return _crossJoin(this, df);
    }

    innerJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`innerJoin takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        // return _innerJoin(this, df, on, leftOn, rightOn);
        return _join("inner", this, df, on, leftOn, rightOn);
    }

    leftJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`leftJoin takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        // return _leftJoin(this, df, on, leftOn, rightOn);
        return _join("left", this, df, on, leftOn, rightOn);
    }

    rightJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`rightJoin takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        // return _rightJoin(this, df, on, leftOn, rightOn);
        return _join("right", this, df, on, leftOn, rightOn);
    }

    groupBy(cols, agg) {
        // Returns a GroupBy object
        // return (new GroupBy(this, cols)).groups;
        return groupAggregation(this, cols, agg);
    }

    orderBy(cols, order) {
        // Returns a new Dataframe with rows ordered by columns
        // defined in the `cols` array. These can be "asc" or "desc",
        // as defined for each corresponding element in the `order`
        // array.
        // e.g. df.orderBy(["col1", "col2"], ["asc", "desc"])
        if (!(cols instanceof Array) || cols.length < 1) {
            throw Error("orderBy requires an array of at least one column, and an optional array defining the order.");
        }
        return new DataFrame(lodash.orderBy(this.rows, cols, order || []), this.columns);
    }

    window(groupByCols, orderByCols, agg) {
        /**
         * @output
         * Dataframe with new columns defined as the result
         * of the aggregations from the agg object applied to each
         * group of rows defined by `groupByCols` and optionally
         * sorted as defined by `orderByCols`.
         * Note: the window function needs to return a single value when
         * called for each group.
         *
         * @inputs
         *
         */
        return groupSortAggregation(this, groupByCols, orderByCols || [], agg);
    }

}
