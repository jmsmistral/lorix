import lodash from 'lodash';

import {
    _crossJoin,
    _innerJoin,
    _leftJoin,
    _rightJoin
} from './joins.js';

import {
    _getUniqueObjectProperties,
} from './utils.js';



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

    col(column) {
        console.log(`.col(${column})`);
        if (this.columns.includes(column)) {
            console.log(`col ${column} exists`);
            let fn = function getCol(row) {return row[column]}
            return fn;
        }
        throw Error(`Column '${column}' does not exist in Dataframe`);
    }

    _evalDataframeExpression(expr) {
        let evaldExpr = eval(expr);
        let fn = function evalExpr(row) {evaldExpr};
        return fn;
    }

    withColumn(col, expr) {
        // Define a new column to a Dataframe
        // Check that `col` is a string that does not start with a number
        let rowFn = this._evalDataframeExpression(expr);
        for (let row of this) {
            console.log(row);
            console.log(rowFn(row));
        }
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
        return _innerJoin(this, df, on, leftOn, rightOn);
    }

    leftJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`leftJoin takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        return _leftJoin(this, df, on, leftOn, rightOn);
    }

    rightJoin(df, leftOn, rightOn) {
        if (arguments.length < 2 || arguments.length > 3) {
            throw Error(`rightJoin takes either two or three arguments. Arguments passed: ${arguments.length}`);
        }
        let on;
        if (arguments.length == 2) on = leftOn;
        return _rightJoin(this, df, on, leftOn, rightOn);
    }

    head() {
        console.table(this.rows, this.columns);
    }
}
