function _getRandomString() {
    return Math.random().toString(36).substring(2, 15);
}

function _generateRandomValue() {
    return (_getRandomString() + _getRandomString());
}

export class DummyNonEqualDataFrame {
    // This is a dummy DataFrame for the purpose
    // of figuring-out the columns being accessed in
    // the join condition function for any join.
    // This enables indexing for faster joins.
    // The constructor only takes an array of columns
    // as input.
    constructor(cols) {
        this.columns = cols;
        this.rows = [this.generateDummyRows(cols)];
    }

    generateDummyRows(cols) {
        // Generates a single row with
        // all the properties (columns)
        // in the dataframe set to false:
        // this.rows = [{col1: false, col2: false, ...}]
        // Then define accessor methods for
        // each property that toggles the flag
        // when accessed.
        let dummyRow = new Proxy({}, {
            // Define getter method for each property (column)
            // that toggles the column flag to true if accessed
            // by the join function.
            get: function(obj, col) {
                // Throw error if referencing a column that
                // does not exist in the DataFrame.
                if (!(cols.includes(col))) {
                    throw Error(`Column ${col} does not exist in DataFrame.`)
                }

                obj[col] = true;
                // Return a random string, to attempt to get all
                // columns being accessed in a function join condition.
                // e.g. If we always return the same value, conditions after
                // an OR comparison in a function would not register as
                // a column being accessed in the join condition.
                return _generateRandomValue();
            }
        });

        return dummyRow;
    }

    getAccessedColumns() {
        // Overwrite Proxy for dummy row to avoid
        // setting all columns to true when trying
        // to extract accessed columns.
        let dummyRow = new Proxy (this.rows[0], {
            get: function(obj, prop) { return obj[prop]; }
        });

        let accessedCols = [];
        for (let col in dummyRow) {
            if (dummyRow[col]) {
                accessedCols.push(col);
            }
        }
        return accessedCols;
    }
}

export class DummyEqualDataFrame {
    // This is a dummy DataFrame for the purpose
    // of figuring-out the columns being accessed in
    // the join condition function for any join.
    // This enables indexing for faster joins.
    // The constructor only takes an array of columns
    // as input.
    constructor(cols) {
        this.columns = cols;
        this.rows = [this.generateDummyRows(cols)];
    }

    generateDummyRows(cols) {
        // Generates a single row with
        // all the properties (columns)
        // in the dataframe set to false:
        // this.rows = [{col1: false, col2: false, ...}]
        // Then define accessor methods for
        // each property that toggles the flag
        // when accessed.
        let dummyRow = new Proxy({}, {
            // Define getter method for each property (column)
            // that toggles the column flag to true if accessed
            // by the join function.
            get: function(obj, col) {
                // Throw error if referencing a column that
                // does not exist in the DataFrame.
                if (!(cols.includes(col))) {
                    throw Error(`Column ${col} does not exist in DataFrame.`)
                }

                obj[col] = true;
                return obj[col];
            }
        });

        return dummyRow;
    }

    getAccessedColumns() {
        // Overwrite Proxy for dummy row to avoid
        // setting all columns to true when trying
        // to extract accessed columns.
        let dummyRow = new Proxy (this.rows[0], {
            get: function(obj, prop) { return obj[prop]; }
        });

        let accessedCols = [];
        for (let col in dummyRow) {
            if (dummyRow[col]) {
                accessedCols.push(col);
            }
        }
        return accessedCols;
    }
}
