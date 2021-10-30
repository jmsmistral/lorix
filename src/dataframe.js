import lodash from "lodash";

import {
    _crossJoin,
    _join
} from "./joins.js";

import {
    groupAggregation
} from "./groups.js"

import {
    applyWindowFunction
} from "./window.js"

import {
    validateFunctionReferencesWithProxy,
    _getUniqueObjectProperties,
    _isSubsetArray,
    _isValidColumnName,
    _getArrayOfObjectReferences,
    _getDistinctFn
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
        if (!_isValidColumnName(col))
            throw Error(`Column name "${col}" is not valid.`);

        // Check that `expr` is a function.
        if ((expr == undefined) || !(expr instanceof Function))
            throw Error("expr provided to withColumn needs to be a function.")

        // Check if function is a window function or not
        if (expr.hasOwnProperty("isWindow")) {
            console.log(expr());
            // const windowParams = expr();
            return applyWindowFunction(this, ...expr());
        }
        else {
            // Check what existing columns are being referenced,
            // if any, and throw an error if at least one does not exist.
            validateFunctionReferencesWithProxy(expr, this.columns);
            let newRows = this.rows.map((row) => ({...row, ...{[col]: expr(row)}}));
            return new DataFrame(newRows, this.columns.concat([col]));
        }
    }

    filter(expr) {
        // Returns a new Dataframe with rows filtered according
        // to the function `expr`.
        // Note: if a reference is made to a non-existent column
        // an error will be thrown.

        // Check number of arguments.
        if (arguments.length < 1 || arguments.length > 1) {
            throw Error(`filter() takes a single argument. Arguments passed: ${arguments.length}`);
        }

        // Check that `expr` is a function.
        if((expr == undefined) || !(expr instanceof Function)) {
            throw Error("`expr` provided to filter needs to be a function.")
        }

        // Check what existing columns are being referenced,
        // if any, and throw an error if at least one does not exist.
        validateFunctionReferencesWithProxy(expr, this.columns);

        let newRows = this.rows.filter((row) => expr(row));
        return new DataFrame(newRows, this.columns);
    }

    distinct(subset=[]) {
        // Return a new DataFrame with duplicate rows dropped.
        // If `subset` of columns is not passed, then duplicates
        // will be identified across all columns.

        // Check number of arguments.
        if (arguments.length > 1)
            throw Error(`distinct() takes a single argument. Arguments passed: ${arguments.length}`);

        if (!(subset instanceof Array))
            throw Error("`subset` provided to distinct needs to be a function.")

        // Check that all columns specified in `subset` exist in
        // the DataFrame.
        if ((subset.length) && !(_isSubsetArray(subset, this.columns)))
            throw Error(`Invalid columns specified in distinct(): ${lodash.difference(subset, this.columns)}`);

        if (subset.length) {
            return new DataFrame(lodash.uniqBy(this.rows, _getDistinctFn(subset)), this.columns);
        } else {
            // If a subset of columns isn't provided, remove
            // duplicate rows across all DataFrame columns
            return new DataFrame(lodash.uniqBy(this.rows, _getDistinctFn(this.columns)), this.columns);
        }
    }

    crossJoin(df) {
        if (arguments.length < 1 || arguments.length > 1) {
            throw Error(`crossJoin() takes a single argument. Arguments passed: ${arguments.length}`);
        }
        return _crossJoin(this, df);
    }

    innerJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`innerJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("inner", this, df, on, leftOn, rightOn);
    }

    leftJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`leftJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("left", this, df, on, leftOn, rightOn);
    }

    rightJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`rightJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("right", this, df, on, leftOn, rightOn);
    }

    orderBy(cols, order) {
        // Returns a new Dataframe with rows ordered by columns
        // defined in the `cols` array. These can be "asc" or "desc",
        // as defined for each corresponding element in the `order`
        // array.
        // e.g. df.orderBy(["col1", "col2"], ["asc", "desc"])
        if (!(cols instanceof Array) || !(cols.length)) {
            throw Error(`orderBy() requires non-empty array of columns.`);
        }
        if (arguments.length == 2 && (!(order instanceof Array) || !(order.length))) {
            throw Error(`orderBy() requires an optional non-empty sort order array.`);
        }

        // Check column array validity
        if (!(_isSubsetArray(cols, this.columns))) {
            throw Error(`Invalid columns found in orderBy(): ${lodash.difference(cols, this.columns)}`);
        }
        if (arguments.length == 2 && !(_isSubsetArray(order, ["asc", "desc"]))) {
            throw Error(`Invalid columns found in orderBy(): ${lodash.difference(order, ["asc", "desc"])}`);
        }

        return new DataFrame(lodash.orderBy(this.rows, cols, order || []), this.columns);
    }

    groupBy(cols, agg) {
        // Return a Map or a DataFrame depending on whether
        // `agg` is defined or not.
        if (!(cols instanceof Array) || !(cols.length)) {
            throw Error(`groupBy() requires non-empty array of columns.`);
        }

        // Check column array validity
        if (!(_isSubsetArray(cols, this.columns))) {
            throw Error(`Invalid columns found in groupBy(): ${lodash.difference(cols, this.columns)}`);
        }

        return groupAggregation(this, cols, agg);
    }

}
