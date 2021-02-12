import lodash from 'lodash';

export class DummyDataFrame {
    // This is a dummy dataframe for the purpose
    // of figuring-out the columns being accessed in
    // the join condition function for any join.
    // This enables indexing for faster joins.
    // The constructor only takes an array of columns
    // as input.
    constructor(cols) {
        this.rows = [this.generateDummyRows(cols)];
        this.columns = cols;
    }

    generateDummyRows(cols) {
        // Generates a single row with
        // all the properties (columns)
        // in the dataframe set to false:
        // this.rows = [{col1: false, col2: false, ...}]
        // Then define accessor methods for
        // each property that toggles the flag
        // when accessed.
        let dummyRow = {};
        for (let col of cols) {
            // Set flag to false as default
            dummyRow["col_" + col] = false;
            // Define getter method for each property (column)
            // that toggles the column flag to true if accessed
            // by the join function.
            Object.defineProperty(dummyRow, col, {
                get() {
                    this["col_" + col] = true;
                    return this["col_" + col];
                }
            });
        }
        return dummyRow;
    }

    getAccessedColumns() {
        // Returns an array of columns accessed
        let accessedCols = [];
        for (let col in this.rows[0]) {
            if (this.rows[0][col]) {
                accessedCols.push(col.replace("col_", ""));
            }
        }
        return accessedCols;
    }
}

export function _isColumnArrayInDataframe(dfCols, groupByCols) {
    if (groupByCols.length < 1) return false;
    if (lodash.difference(groupByCols, dfCols).length == 0) {
        return true
    }
    return false;
}

export function _isString(val) {
    return Object.prototype.toString.call(val) === "[object String]";
}

export function _isValidColumnName(col) {
    if (_isString(col) && (/[a-zA-Z_]/).test(col[0])) {
        return true;
    }
    return false;
}

export function _getUniqueObjectProperties(arr) {
    if (arr instanceof Array) {
        let objectProperties = arr.reduce((output, cols) => {
            if (
                Object.keys(output).length == 0 ||
                lodash.isEqual(lodash.sortBy(Object.keys(output)), lodash.sortBy(Object.keys(cols)))
            ) {
                return Object.assign(output, cols)
            }
            throw Error("_getUniqueObjectProperties() columns not equal across rows.");
        }, {});
        return Object.keys(objectProperties);
    }
    throw Error("_getUniqueObjectProperties() only accepts an array of objects.");
}
