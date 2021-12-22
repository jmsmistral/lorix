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
    _getDistinctFn,
    _isString
} from "./utils.js";



export class DataFrame {
    constructor(rowArray=[], columns=[]) {

        // Check that row properties align with specified columns
        if (rowArray.length && columns.length) {
            const rowColumns = _getUniqueObjectProperties(rowArray);
            const diff = lodash.difference(rowColumns, columns)
            if (diff.length)
                throw Error(`There are differences between row properties and specified columns: '${diff.join(', ')}'`);
        }

        this.rows = rowArray;
        this.columns = columns;
    }

    // Enable to iterate over DataFrame rows
    *iterator() {
        for (let row of this.rows)
            yield row;
    }

    [Symbol.iterator]() {
        return this.iterator();
    }

    static fromArray(arr) {
        // Return a Dataframe from an array of objects.
        if (arr instanceof Array) {
            let cols = _getUniqueObjectProperties(arr);
            if (cols.length > 0)
                return new DataFrame(arr, cols);
        }
        throw Error("Dataframe.fromArray() only accepts a non-empty array of objects.");
    }

    toArray() {
        return this.rows;
    }

    head(n=10) {
        if (n > 0)
            console.table(this.rows.slice(0, n), this.columns);
    }

    slice(i=0, j=-1) {
        // Returns a DataFrame with the subset of rows
        // specified by the i, j indexes.
        if (j !== -1)
            return new DataFrame(this.rows.slice(i, j + 1), this.columns);
        return new DataFrame(this.rows.slice(i), this.columns);
    }

    select(...fields) {
        if (!fields.length)
            throw Error("No columns provided to select().");

        // Check that fields passed exist in DataFrame
        const diff = lodash.difference(fields, this.columns)
        if (diff.length)
            throw Error(`Field(s) '${diff.join(', ')}' do not exist in DataFrame.`);

        const outputArray = [];
        for (const row of this.rows)
            outputArray.push(lodash.pick(row, fields))

        return new DataFrame(outputArray, fields);
    }

    drop(...fields) {
        if (!fields.length)
            throw Error("No columns provided to drop().");

        // Check that fields passed exist in DataFrame
        const diff = lodash.difference(fields, this.columns)
        if (diff.length)
            throw Error(`Field(s) '${diff.join(', ')}' do not exist in DataFrame.`);

        const outputArray = [];
        for (const row of this.rows)
            outputArray.push(lodash.omit(row, fields))

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
            throw Error("expr provided to withColumn needs to be a function.");

        // Check if `expr` is a window function, and
        // apply it to the DataFrame, if so.
        if (expr.hasOwnProperty("isWindow"))
            return applyWindowFunction(this, col, ...expr());

        // Check what existing columns are being referenced,
        // and throw an error if at least one does not exist.
        validateFunctionReferencesWithProxy(expr, this.columns);
        let newRows = this.rows.map((row) => ({...row, ...{[col]: expr(row)}}));
        let newCols = this.columns.includes(col) ? this.columns : this.columns.concat([col]);
        return new DataFrame(newRows, newCols);
    }

    filter(expr) {
        // Returns a new Dataframe with rows filtered according
        // to the function `expr`.
        // Note: if a reference is made to a non-existent column
        // an error will be thrown.

        // Check number of arguments.
        if (arguments.length < 1 || arguments.length > 1)
            throw Error(`filter() takes a single argument. Arguments passed: ${arguments.length}`);

        // Check that `expr` is a function.
        if((expr == undefined) || !(expr instanceof Function))
            throw Error("`expr` provided to filter needs to be a function.");

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
            throw Error("`subset` provided to distinct needs to be a function.");

        // Check that all columns specified in `subset` exist in the DataFrame.
        if ((subset.length) && !(_isSubsetArray(subset, this.columns)))
            throw Error(`Invalid columns specified in distinct(): ${lodash.difference(subset, this.columns)}`);

        if (subset.length)
            return new DataFrame(lodash.uniqBy(this.rows, _getDistinctFn(subset)), this.columns);

        // If a subset of columns isn't provided, remove
        // duplicate rows across all DataFrame columns
        return new DataFrame(lodash.uniqBy(this.rows, _getDistinctFn(this.columns)), this.columns);
    }

    regexReplace(cols, replaceRegex, newString) {
        /**
         * Returns a new DataFrame with regular expression
         * `replaceRegex` replaced by `newString` in
         * one or more columns defined in Array `cols`.
         */

        // Check number of arguments.
        if (arguments.length < 3 || arguments.length > 3)
            throw Error(`regexReplace() takes three arguments. Number of arguments passed: ${arguments.length}`);

        // Check that `cols` is an Array.
        if (!(cols instanceof Array))
            throw Error("First parameter provided to regexReplace() needs to be an array of columns.");

        // Check that `replaceRegex` is a regular expression.
        if (!(replaceRegex instanceof RegExp))
            throw Error("Second parameter provided to regexReplace() needs to be a regular expression.");

        // Check that `newString` is a string.
        if (!_isString(newString))
            throw Error("Third parameter provided to regexReplace() needs to be a string.");

        // Check that columns in `cols` are valid.
        const diff = lodash.difference(cols, this.columns)
        if (diff.length)
            throw Error(`Invalid columns provided in regexReplace(): '${diff.join(', ')}'`);

        // Replace strings in columns.
        let newRows = lodash.cloneDeep(this.rows);
        for (let col of cols) {
            newRows = newRows.map((row) => {
                try {
                    row[col] = row[col].replace(replaceRegex, newString);
                    return row;
                } catch(error) { /* pass if replacing a non-string */ }
            });
        }

        return new DataFrame(newRows, this.columns);
    }

    replaceAll(cols, oldString, newString) {
        /**
         * Returns a new DataFrame with all instances of
         * string `oldString` replaced by `newString` in
         * one or more columns defined in Array `cols`.
         */

        // Check number of arguments.
        if (arguments.length < 3 || arguments.length > 3)
            throw Error(`replaceAll() takes three arguments. Number of arguments passed: ${arguments.length}`);

        // Check that `cols` is an Array.
        if (!(cols instanceof Array))
            throw Error("`cols` provided to replaceAll() needs to be an array of columns.");

        // Check that `oldString` and `newString` are a strings.
        if (!_isString(oldString) || !_isString(newString))
            throw Error("Second and third parameters provided to replaceAll() need to be strings.");

        // Check that columns in `cols` are valid.
        const diff = lodash.difference(cols, this.columns)
        if (diff.length)
            throw Error(`Invalid columns provided in replaceAll(): '${diff.join(', ')}'`);

        // Replace strings in columns.
        let newRows = lodash.cloneDeep(this.rows);
        for (let col of cols) {
            newRows = newRows.map((row) => {
                try {
                    row[col] = row[col].replaceAll(oldString, newString);
                    return row;
                } catch(error) { /* pass if replacing a non-string */ }
            });
        }

        return new DataFrame(newRows, this.columns);
    }

    replace(cols, oldString, newString) {
        /**
         * Returns a new DataFrame with first instance of
         * string `oldString` replaced by `newString` in
         * one or more columns defined in Array `cols`.
         */

        // Check number of arguments.
        if (arguments.length < 3 || arguments.length > 3)
            throw Error(`replace() takes three arguments. Number of arguments passed: ${arguments.length}`);

        // Check that `cols` is an Array.
        if(!(cols instanceof Array))
            throw Error("`cols` provided to replace() needs to be an array of columns.");

        // Check that `oldString` and `newString` are a strings.
        if (!_isString(oldString) || !_isString(newString))
            throw Error("Second and third parameters provided to replace() need to be strings.");

        // Check that columns in `cols` are valid.
        const diff = lodash.difference(cols, this.columns)
        if (diff.length)
            throw Error(`Invalid columns provided in replace(): '${diff.join(', ')}'`);

        // Replace strings in columns.
        let newRows = lodash.cloneDeep(this.rows);
        for (let col of cols) {
            newRows = newRows.map((row) => {
                try {
                    row[col] = row[col].replace(oldString, newString);
                    return row;
                } catch(error) { /* pass if replacing a non-string */ }
            });
        }

        return new DataFrame(newRows, this.columns);
    }

    crossJoin(df) {
        if (arguments.length < 1 || arguments.length > 1)
            throw Error(`crossJoin() takes a single argument. Arguments passed: ${arguments.length}`);

        return _crossJoin(this, df);
    }

    innerJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3)
            throw Error(`innerJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);

        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("inner", this, df, on, leftOn, rightOn);
    }

    leftJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3)
            throw Error(`leftJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);

        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("left", this, df, on, leftOn, rightOn);
    }

    leftAntiJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3)
            throw Error(`leftAntiJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);

        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("leftAnti", this, df, on, leftOn, rightOn);
    }

    rightJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3)
            throw Error(`rightJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);

        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("right", this, df, on, leftOn, rightOn);
    }

    rightAntiJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3)
            throw Error(`rightAntiJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);

        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("rightAnti", this, df, on, leftOn, rightOn);
    }

    fullOuterJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3)
            throw Error(`fullOuterJoin() takes either two or three arguments. Arguments passed: ${arguments.length}`);

        let on;
        if (arguments.length == 2) on = leftOn;
        return _join("fullOuter", this, df, on, leftOn, rightOn);
    }

    orderBy(cols, order) {
        // Returns a new Dataframe with rows ordered by columns
        // defined in the `cols` array. These can be "asc" or "desc",
        // as defined for each corresponding element in the `order`
        // array.
        // e.g. df.orderBy(["col1", "col2"], ["asc", "desc"])

        if (!(cols instanceof Array) || !(cols.length))
            throw Error(`orderBy() requires non-empty array of columns.`);

        if (arguments.length == 2 && (!(order instanceof Array) || !(order.length)))
            throw Error(`orderBy() requires an optional non-empty sort order array.`);

        // Check column array validity
        if (!(_isSubsetArray(cols, this.columns)))
            throw Error(`Invalid columns found in orderBy(): ${lodash.difference(cols, this.columns)}`);

        if (arguments.length == 2 && !(_isSubsetArray(order, ["asc", "desc"])))
            throw Error(`Invalid columns found in orderBy(): ${lodash.difference(order, ["asc", "desc"])}`);

        return new DataFrame(lodash.orderBy(this.rows, cols, order || []), this.columns);
    }

    groupBy(cols, agg) {
        // Return a Map or a DataFrame depending on whether
        // `agg` is defined or not.
        if (!(cols instanceof Array) || !(cols.length))
            throw Error(`groupBy() requires non-empty array of columns.`);

        // Check column array validity
        if (!(_isSubsetArray(cols, this.columns)))
            throw Error(`Invalid columns found in groupBy(): ${lodash.difference(cols, this.columns)}`);

        return groupAggregation(this, cols, agg);
    }

}
