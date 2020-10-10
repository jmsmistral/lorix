
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
