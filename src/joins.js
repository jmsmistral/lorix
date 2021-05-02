import d3Array from 'd3-array';
import lodash from 'lodash';

import { DataFrame } from './dataframe.js';
import { DummyDataFrame } from './utils.js';


function _cleanCommonCols(leftRow, rightRow, commonCols) {
    let cleanLeft = {...leftRow};
    let cleanRight = {...rightRow};
    for (let col of commonCols) {
        cleanLeft[col + "_x"] = cleanLeft[col];
        cleanRight[col + "_y"] = cleanRight[col];
        delete cleanLeft[col], delete cleanRight[col];
        return {...cleanLeft, ...cleanRight};
    }
}

function _getRightJoinColumns(leftDf, rightDf, on) {
    // Returns an array with the columns referenced
    // from the right dataframe in the join condition.
    // This is done by running the join function with
    // dummy objects. These have special accessor methods
    // that log the properties being accessed.
    let leftDummyDf = new DummyDataFrame(leftDf.columns);
    let rightDummyDf = new DummyDataFrame(rightDf.columns);
    on(leftDummyDf.rows[0], rightDummyDf.rows[0]);
    return rightDummyDf.getAccessedColumns();
}

function _getLeftJoinColumns(leftDf, rightDf, on) {
    // Returns an array with the columns referenced
    // from the left dataframe in the join condition.
    // This is done by running the join function with
    // dummy objects. These have special accessor methods
    // that log the properties being accessed.
    let leftDummyDf = new DummyDataFrame(leftDf.columns);
    let rightDummyDf = new DummyDataFrame(rightDf.columns);
    on(leftDummyDf.rows[0], rightDummyDf.rows[0]);
    return rightDummyDf.getAccessedColumns();
}

export function _crossJoin(df1, df2) {
    let commonCols = df1.columns.filter(col => df2.columns.includes(col));
    let rowArray = d3Array.cross(df1, df2, (left, right) => {
        if (commonCols.length) return _cleanCommonCols(left, right, commonCols);
        return {...left, ...right};
    }).filter(Boolean);
    let outputColumns = rowArray.length ? Object.getOwnPropertyNames(rowArray[0]) : df1.columns.concat(df2.columns);
    return new DataFrame(rowArray, outputColumns);
}

export function _innerJoin(leftDf, rightDf, on, leftOn, rightOn) {
    console.log("_innerJoin()");
    // Check that a join condition exists
    if (!on && !(leftOn || rightOn)) {
        throw Error("Need to specify either 'on', or both 'leftOn' and 'rightOn'");
    }

    if (on) {
        // Determine if on is a function or array or undefined
        // and run either a non-indexed or indexed join
        if (on instanceof Function) {
            return _nonIndexedInnerJoin(leftDf, rightDf, on);
        }

        if (on instanceof Array) {
            return _indexedInnerJoin(leftDf, rightDf, on, leftOn, rightOn);
        }
        throw Error("'on' needs to be either a function or an array");
    }

    if ((leftOn instanceof Array) && (rightOn instanceof Array)) {
        console.log("leftOn and rightOn defined");
        return _indexedInnerJoin(leftDf, rightDf, on, leftOn, rightOn);
    }
    throw Error("'leftOn' and 'rightOn' need to be arrays");
};

export function _leftJoin(leftDf, rightDf, on, leftOn, rightOn) {
    console.log("_leftJoin()");
    // Check that a join condition exists
    if (!on && !(leftOn || rightOn)) {
        throw Error("Need to specify either 'on', or both 'leftOn' and 'rightOn'");
    }

    if (on) {
        // Determine if on is a function or array or undefined
        // and run either a non-indexed or indexed join
        if (on instanceof Function) {
            return _nonIndexedLeftJoin(leftDf, rightDf, on);
        }

        if (on instanceof Array) {
            return _indexedLeftJoin(leftDf, rightDf, on, leftOn, rightOn);
        }
        throw Error("'on' needs to be either a function or an array");
    }

    if ((leftOn instanceof Array) && (rightOn instanceof Array)) {
        console.log("leftOn and rightOn defined");
        return _indexedLeftJoin(leftDf, rightDf, on, leftOn, rightOn);
    }
    throw Error("'leftOn' and 'rightOn' need to be arrays")
};

export function _rightJoin(leftDf, rightDf, on, leftOn, rightOn) {
    console.log("_rightJoin()");
    // Check that a join condition exists
    if (!on && !(leftOn || rightOn)) {
        throw Error("Need to specify either 'on', or both 'leftOn' and 'rightOn'");
    }

    if (on) {
        // Determine if on is a function or array or undefined
        // and run either a non-indexed or indexed join
        if (on instanceof Function) {
            return _nonIndexedRightJoin(leftDf, rightDf, on);
        }

        if (on instanceof Array) {
            return _indexedRightJoin(leftDf, rightDf, on, leftOn, rightOn);
        }
        throw Error("'on' needs to be either a function or an array");
    }

    if ((leftOn instanceof Array) && (rightOn instanceof Array)) {
        console.log("leftOn and rightOn defined");
        return _indexedRightJoin(leftDf, rightDf, on, leftOn, rightOn);
    }
    throw Error("'leftOn' and 'rightOn' need to be arrays");
};

function _nonIndexedInnerJoin(leftDf, rightDf, on) {
    // Returns a DataFrame representing the join
    // of leftDf and rightDf DataFrames, based on the
    // join condition function provided via the "on"
    // parameter. This function takes two rows, and
    // returns true if the rows match, and false
    // otherwise.
    console.log("_nonIndexedInnerJoin()");
    const joinCols = _getRightJoinColumns(leftDf, rightDf, on);
    const sortedRightDfRowArray = lodash.orderBy(rightDf.rows, joinCols);

    const outputRowArray = leftDf.rows.flatMap((leftRow) => {
        const matchingRows = sortedRightDfRowArray.filter((rightRow) => on(leftRow, rightRow));
        return matchingRows.map((rightRow) => ({...leftRow, ...rightRow}));
    });

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}

function _nonIndexedLeftJoin(leftDf, rightDf, on) {
    // Returns a DataFrame representing the join
    // of leftDf and rightDf DataFrames, based on the
    // join condition function provided via the "on"
    // parameter. This function takes two rows, and
    // returns true if the rows match, and false
    // otherwise.
    console.log("_nonIndexedLeftJoin()");
    const joinCols = _getRightJoinColumns(leftDf, rightDf, on);
    const sortedRightDfRowArray = lodash.orderBy(rightDf.rows, joinCols);
    let nullRow = _getRightNullRow(leftDf, rightDf);

    const outputRowArray = leftDf.rows.flatMap((leftRow) => {
        const matchingRows = sortedRightDfRowArray.filter((rightRow) => on(leftRow, rightRow));
        if (!matchingRows.length) return {...leftRow, ...nullRow};
        return matchingRows.map((rightRow) => ({...leftRow, ...rightRow}));
    });

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}

function _nonIndexedRightJoin(leftDf, rightDf, on) {
    // Returns a DataFrame representing the join
    // of leftDf and rightDf DataFrames, based on the
    // join condition function provided via the "on"
    // parameter. This function takes two rows, and
    // returns true if the rows match, and false
    // otherwise.
    console.log("_nonIndexedRightJoin()");
    const joinCols = _getLeftJoinColumns(leftDf, rightDf, on);
    const sortedLeftDfRowArray = lodash.orderBy(leftDf.rows, joinCols);
    let nullRow = _getLeftNullRow(leftDf, rightDf);

    const outputRowArray = rightDf.rows.flatMap((rightRow) => {
        const matchingRows = sortedLeftDfRowArray.filter((leftRow) => on(leftRow, rightRow));
        if (!matchingRows.length) return {...nullRow, ...rightRow};
        return matchingRows.map((leftRow) => ({...leftRow, ...rightRow}));
    });

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}

function _indexedInnerJoin(leftDf, rightDf, on, leftOn, rightOn) {
    // Joins two dataframes based on the array
    // of join columns defined either in the "on",
    // or both "leftOn" and "rightOn" parameters.
    // The join condition is based on the equality
    // of the array of columns in the specified order.
    console.log("_indexedInnerJoin()");
    // TODO check that columns in join exist on both dataframes
    // Index right-hand dataframe
    const rightDfIndex = _getIndex(rightDf, (rightOn ? rightOn : on));
    // Perform join using right dataframe index
    const leftDfJoinCols = leftOn ? leftOn : on;
    let outputRowArray = [];
    for (let row of leftDf.rows) {
        let matchingRows = _lookupIndex(row, leftDfJoinCols, rightDfIndex);
        if (matchingRows) {
            for (let matchedRow of matchingRows) {
                outputRowArray.push({...row, ...matchedRow});
            }
            continue;
        }
    }

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}

function _indexedLeftJoin(leftDf, rightDf, on, leftOn, rightOn) {
    // Joins two dataframes based on the array
    // of join columns defined either in the "on",
    // or both "leftOn" and "rightOn" parameters.
    // The join condition is based on the equality
    // of the array of columns in the specified order.
    console.log("_indexedLeftJoin()");
    // TODO check that columns in join exist on both dataframes
    // Index right-hand dataframe
    const rightDfIndex = _getIndex(rightDf, (rightOn ? rightOn : on));
    // Perform join using right dataframe index
    const leftDfJoinCols = leftOn ? leftOn : on;
    // Null row for non-matching rows
    // excluding common properties, as these are
    // overwritten when combining objects
    let nullRow = _getRightNullRow(leftDf, rightDf);
    let outputRowArray = [];
    for (let row of leftDf.rows) {
        let matchingRows = _lookupIndex(row, leftDfJoinCols, rightDfIndex);
        if (matchingRows) {
            for (let matchedRow of matchingRows) {
                outputRowArray.push({...row, ...matchedRow});
            }
            continue;
        }
        outputRowArray.push({...row, ...nullRow});
    }

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}

function _indexedRightJoin(leftDf, rightDf, on, leftOn, rightOn) {
    // Joins two dataframes based on the array
    // of join columns defined either in the "on",
    // or both "leftOn" and "rightOn" parameters.
    // The join condition is based on the equality
    // of the array of columns in the specified order.
    console.log("_indexedRightJoin()");
    // TODO check that columns in join exist on both dataframes
    // Index left-hand dataframe
    const leftDfIndex = _getIndex(leftDf, (leftOn ? leftOn : on));
    // Perform join using right dataframe index
    const rightDfJoinCols = rightOn ? rightOn : on;
    // Null row for non-matching rows
    // excluding common properties, as these are
    // overwritten when combining objects
    let nullRow = _getLeftNullRow(leftDf, rightDf);
    let outputRowArray = [];
    for (let row of rightDf.rows) {
        let matchingRows = _lookupIndex(row, rightDfJoinCols, leftDfIndex);
        if (matchingRows) {
            for (let matchedRow of matchingRows) {
                outputRowArray.push({...matchedRow, ...row});
            }
            continue;
        }
        outputRowArray.push({...nullRow, ...row});
    }

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}

function _getIndex(df, cols) {
    // Generates an index for a dataframe
    // on a specified array of columns.
    // Example:
    // For a given set of columns ["col1", "col2"]
    // the generated index takes the form:
    // Map(
    //     "col1" => Map(
    //         "col2" => [
    //             {col1: a, col2: b, col3: c}  // row
    //             {col1: d, col2: e, col3: f}  // row
    //         ]
    //     )
    // )
    const groupFuncs = [];
    for (let joinCol of cols) {
        // Generate functions for indexing with d3
        groupFuncs.push(arr => arr[joinCol]);
    }
    return d3Array.group(df, ...groupFuncs);
}

function _lookupIndex(row, cols, index) {
    // Recursive function that takes a given row,
    // the columns to join on, and the index to look-up
    // any potential matching rows.
    let joinCols = Array.from(cols);
    let indexValue = index.get(row[joinCols[0]]);
    if (!joinCols.length && !indexValue) return [];
    if (indexValue instanceof Map) {
        joinCols.shift();
        return _lookupIndex(row, joinCols, indexValue);
    } else {
        return indexValue;
    }
}

function _getLeftNullRow(leftDf, rightDf) {
    // Returns an object representing an empty
    // row from the left dataframe. This is used
    // for generating non-matching rows in left
    // and right joins.
    // Excludes common properties, as these are
    // overwritten when combining objects.
    const commonCols = leftDf.columns.filter(col => rightDf.columns.includes(col));
    let nullRow = {};
    for (const col of leftDf.columns) {
        if (commonCols.includes(col)) continue;
        nullRow[col] = null;
    }
    return nullRow;
}

function _getRightNullRow(leftDf, rightDf) {
    // Returns an object representing an empty
    // row from the left dataframe. This is used
    // for generating non-matching rows in left
    // and right joins.
    // Excludes common properties, as these are
    // overwritten when combining objects.
    const commonCols = leftDf.columns.filter(col => rightDf.columns.includes(col));
    let nullRow = {};
    for (const col of rightDf.columns) {
        if (commonCols.includes(col)) continue;
        nullRow[col] = null;
    }
    return nullRow;
}
