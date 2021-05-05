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
        let dummyRow = new  Proxy({}, {
            // Define getter method for each property (column)
            // that toggles the column flag to true if accessed
            // by the join function.
            get: function(obj, prop) {
                // Throw error if referencing a column that
                // does not exist in the DataFrame.
                if (!(cols.includes(prop))) {
                    throw Error(`Column ${prop} does not exist in DataFrame.`)
                }

                obj[prop] = true;
                return obj[prop];
            }
        });

        // Initialise property flags to false
        for (let col of cols)
            dummyRow[col] = false;
        return dummyRow;
    }

    getAccessedColumns() {
        // Returns an array of columns accessed
        let accessedCols = [];
        for (let col in this.rows[0]) {
            if (this.rows[0][col]) {
                accessedCols.push(col);
            }
        }
        return accessedCols;
    }
}

export function _isColumnArrayInDataframe(dfCols, groupByCols) {
    if (groupByCols.length < 1) return false;
    return (lodash.difference(groupByCols, dfCols).length == 0);
}

export function _isString(val) {
    return Object.prototype.toString.call(val) === "[object String]";
}

export function _isValidColumnName(col) {
    return (_isString(col) && (/[a-zA-Z_]/).test(col[0]));
}

export function _getUniqueObjectProperties(arr) {
    // Takes an array of objects, and returns
    // an array of distinct properties across all
    // objects in the array.
    // Throws an error if rows do not all have the
    // same columns.
    if (arr instanceof Array) {
        // First row used to compare other row properties against
        let firstRowProps = lodash.sortBy(Object.keys(arr[0]));

        let objectProperties = arr.reduce((output, cols) => {
            if (
                (Object.keys(output).length == 0) ||
                // lodash.isEqual(lodash.sortBy(Object.keys(output)), lodash.sortBy(Object.keys(cols)))
                (lodash.isEqual(firstRowProps, lodash.sortBy(Object.keys(cols))))
            ) {
                return Object.assign(output, cols)
            }
            throw Error("_getUniqueObjectProperties() columns not equal across rows.");
        }, {});
        return Object.keys(objectProperties);
    }
    throw Error("_getUniqueObjectProperties() only accepts an array of objects.");
}
