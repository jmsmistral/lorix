import lodash from 'lodash';

import {
    _crossJoin,
    _innerJoin,
    _leftJoin,
    _rightJoin
} from './joins.js';


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
