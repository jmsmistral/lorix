import {cross, group} from 'd3-array';
import lodash from 'lodash';

import { DataFrame } from './dataframe.js';
import {
    validateJoinFunctionReferencesWithProxy,
    _isSubsetArray,
    _getInvalidJoinColumns,
    validateOverlappingColumnsArrayJoin,
    validateOverlappingColumnsFunctionJoin
} from './utils.js';


function _cleanCommonCols(leftRow, rightRow, commonCols) {
    let cleanLeft = {...leftRow};
    let cleanRight = {...rightRow};
    for (let col of commonCols) {
        cleanLeft[col + "_x"] = cleanLeft[col];
        cleanRight[col + "_y"] = cleanRight[col];
        delete cleanLeft[col], delete cleanRight[col];
    }
    return {...cleanLeft, ...cleanRight};
}


function _getRightJoinColumns(leftDf, rightDf, on) {
    // Returns an array with the columns referenced
    // from the right dataframe in the join condition.
    // This is done by running the join function with
    // dummy objects. These have special accessor methods
    // that log the properties being accessed.
    return validateJoinFunctionReferencesWithProxy(on, leftDf.columns, rightDf.columns)["right"];
}


function _getLeftJoinColumns(leftDf, rightDf, on) {
    // Returns an array with the columns referenced
    // from the left dataframe in the join condition.
    // This is done by running the join function with
    // dummy objects. These have special accessor methods
    // that log the properties being accessed.
    return validateJoinFunctionReferencesWithProxy(on, leftDf.columns, rightDf.columns)["left"];
}


function _dispatchNonIndexedJoin(type, leftDf, rightDf, on) {
    switch (type) {
        case "inner":
            return _nonIndexedInnerJoin(leftDf, rightDf, on);
        case "left":
            return _nonIndexedLeftJoin(leftDf, rightDf, on, false);
        case "right":
            return _nonIndexedRightJoin(leftDf, rightDf, on, false);
        case "leftAnti":
            return _nonIndexedLeftJoin(leftDf, rightDf, on, true);
        case "rightAnti":
            return _nonIndexedRightJoin(leftDf, rightDf, on, true);
        case "fullOuter":
            return _nonIndexedFullOuterJoin(leftDf, rightDf, on);
    }
}


function _dispatchIndexedJoin(type, leftDf, rightDf, on, leftOn, rightOn) {
    switch (type) {
        case "inner":
            return _indexedInnerJoin(leftDf, rightDf, on, leftOn, rightOn);
        case "left":
            return _indexedLeftJoin(leftDf, rightDf, on, leftOn, rightOn, false);
        case "right":
            return _indexedRightJoin(leftDf, rightDf, on, leftOn, rightOn, false);
        case "leftAnti":
            return _indexedLeftJoin(leftDf, rightDf, on, leftOn, rightOn, true);
        case "rightAnti":
            return _indexedRightJoin(leftDf, rightDf, on, leftOn, rightOn, true);
        case "fullOuter":
            return _indexedFullOuterJoin(leftDf, rightDf, on, leftOn, rightOn);
    }
}


export function _crossJoin(df1, df2) {
    if (df2 instanceof DataFrame) {
        let commonCols = df1.columns.filter(col => df2.columns.includes(col));
        let rowArray = cross(df1, df2, (left, right) => {
            if (commonCols.length) return _cleanCommonCols(left, right, commonCols);
            return {...left, ...right};
        }).filter(Boolean); // Used to filter-out any undefined
        let outputColumns = rowArray.length ? Object.getOwnPropertyNames(rowArray[0]) : df1.columns.concat(df2.columns);
        return new DataFrame(rowArray, outputColumns);
    }
    throw Error("crossJoin() expects another DataFrame");
}


export function _join(type="inner", leftDf, rightDf, on, leftOn, rightOn) {
    if (!(rightDf instanceof DataFrame)) {
        throw Error("A join can only be performed on another DataFrame");
    }
    // Check that a join condition exists
    if (!on && !(leftOn || rightOn)) {
        throw Error("Invalid join. Specify either 'on', or both 'leftOn' and 'rightOn'");
    }

    // Determine if condition is a
    // - function (non-indexed join)
    // - single array (indexed join)
    if (on) {
        if (on instanceof Function) {
            // Check that the function references valid columns
            // and error if at least one is invalid
            let left, right, both;
            ({left, right, both} = validateJoinFunctionReferencesWithProxy(on, leftDf.columns, rightDf.columns));
            validateOverlappingColumnsFunctionJoin(leftDf.columns, rightDf.columns, left, right);
            return _dispatchNonIndexedJoin(type, leftDf, rightDf, on);
        }

        if (on instanceof Array && on.length) {
            // Single array of columns provided (no rightOn)
            // Check that all specified columns exit in both DataFrames.
            if (_isSubsetArray(on, leftDf.columns) && _isSubsetArray(on, rightDf.columns)) {
                validateOverlappingColumnsArrayJoin(leftDf.columns, rightDf.columns, on);
                return _dispatchIndexedJoin(type, leftDf, rightDf, on, leftOn, rightOn);
            }
            let invalidCols = _getInvalidJoinColumns(leftDf.columns, rightDf.columns, on);
            throw Error(`Invalid columns found in join condition: ${invalidCols}`);
        }
        throw Error("'on' needs to be either a function or a non-empty array");
    }

    // Condition made-up of two arrays (leftOn and rightOn)
    if ((leftOn instanceof Array && leftOn.length) && (rightOn instanceof Array && rightOn.length) && (leftOn.length == rightOn.length)) {
        // Check that all specified columns exit in both DataFrames.
        if (_isSubsetArray(leftOn, leftDf.columns) && _isSubsetArray(rightOn, rightDf.columns)) {
            validateOverlappingColumnsArrayJoin(leftDf.columns, rightDf.columns, leftOn, rightOn);
            return _dispatchIndexedJoin(type, leftDf, rightDf, on, leftOn, rightOn);
        }
        let invalidCols = _getInvalidJoinColumns(leftDf.columns, rightDf.columns, leftOn, rightOn);
        throw Error(`Invalid columns found in join condition: ${invalidCols}`);
    }
    throw Error("'leftOn' and 'rightOn' need to be non-empty arrays of equal length");
};


function _nonIndexedInnerJoin(leftDf, rightDf, on) {
    // Returns a DataFrame representing the join
    // of leftDf and rightDf DataFrames, based on the
    // join condition function provided via the "on"
    // parameter. This function takes two rows, and
    // returns true if the rows match, and false
    // otherwise.
    const joinCols = _getRightJoinColumns(leftDf, rightDf, on);
    const sortedRightDfRowArray = lodash.orderBy(rightDf.rows, joinCols);

    const outputRowArray = leftDf.rows.flatMap((leftRow) => {
        const matchingRows = sortedRightDfRowArray.filter((rightRow) => on(leftRow, rightRow));
        return matchingRows.map((rightRow) => ({...leftRow, ...rightRow}));
    });

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}


function _nonIndexedLeftJoin(leftDf, rightDf, on, isAntiJoin=false) {
    // Returns a DataFrame representing the join
    // of leftDf and rightDf DataFrames, based on the
    // join condition function provided via the "on"
    // parameter. This function takes two rows, and
    // returns true if the rows match, and false
    // otherwise.
    const joinCols = _getRightJoinColumns(leftDf, rightDf, on);
    const sortedRightDfRowArray = lodash.orderBy(rightDf.rows, joinCols);
    let nullRow = _getRightNullRow(leftDf, rightDf);

    const outputRowArray = leftDf.rows.flatMap((leftRow) => {
        const matchingRows = sortedRightDfRowArray.filter((rightRow) => on(leftRow, rightRow));
        if (!matchingRows.length) return {...leftRow, ...nullRow};
        if (!isAntiJoin)
            return matchingRows.map((rightRow) => ({...leftRow, ...rightRow}));
        return [];
    });

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}


function _nonIndexedRightJoin(leftDf, rightDf, on, isAntiJoin=false) {
    // Returns a DataFrame representing the join
    // of leftDf and rightDf DataFrames, based on the
    // join condition function provided via the "on"
    // parameter. This function takes two rows, and
    // returns true if the rows match, and false
    // otherwise.
    const joinCols = _getLeftJoinColumns(leftDf, rightDf, on);
    const sortedLeftDfRowArray = lodash.orderBy(leftDf.rows, joinCols);
    let nullRow = _getLeftNullRow(leftDf, rightDf);

    const outputRowArray = rightDf.rows.flatMap((rightRow) => {
        const matchingRows = sortedLeftDfRowArray.filter((leftRow) => on(leftRow, rightRow));
        if (!matchingRows.length) return {...nullRow, ...rightRow};
        if (!isAntiJoin)
            return matchingRows.map((leftRow) => ({...leftRow, ...rightRow}));
        return [];
    });

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}


function _nonIndexedFullOuterJoin(leftDf, rightDf, on) {
    // Returns a DataFrame representing the join
    // of leftDf and rightDf DataFrames, based on the
    // join condition function provided via the "on"
    // parameter. This function takes two rows, and
    // returns true if the rows match, and false
    // otherwise.
    let dfLeftJoin = _nonIndexedLeftJoin(leftDf, rightDf, on, false);
    let dfRightAntiJoin = _nonIndexedRightJoin(leftDf, rightDf, on, true);
    return new DataFrame([...dfLeftJoin.rows, ...dfRightAntiJoin.rows], dfLeftJoin.columns);
}


function _indexedInnerJoin(leftDf, rightDf, on, leftOn, rightOn) {
    // Joins two dataframes based on the array
    // of join columns defined either in the "on",
    // or both "leftOn" and "rightOn" parameters.
    // The join condition is based on the equality
    // of the array of columns in the specified order.

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


function _indexedLeftJoin(leftDf, rightDf, on, leftOn, rightOn, isAnti=false) {
    // Joins two dataframes based on the array
    // of join columns defined either in the "on",
    // or both "leftOn" and "rightOn" parameters.
    // The join condition is based on the equality
    // of the array of columns in the specified order.

    // Index right-hand dataframe
    const rightDfIndex = _getIndex(rightDf, (rightOn ? rightOn : on));
    // Perform join using right dataframe index
    const leftDfJoinCols = leftOn ? leftOn : on;
    // Null row for non-matching rows
    // excluding common properties, as these are
    // overwritten when combining objects
    let nullRow = _getRightNullRow(leftDf, rightDf);
    let outputRowArray = [];
    for (let leftRow of leftDf.rows) {
        let matchingRows = _lookupIndex(leftRow, leftDfJoinCols, rightDfIndex);
        if (!matchingRows) {
            outputRowArray.push({...leftRow, ...nullRow});
            continue;
        }
        if (!isAnti) {
            for (let matchedRow of matchingRows)
                outputRowArray.push({...leftRow, ...matchedRow});
        }
    }

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}


function _indexedRightJoin(leftDf, rightDf, on, leftOn, rightOn, isAnti=false) {
    // Joins two dataframes based on the array
    // of join columns defined either in the "on",
    // or both "leftOn" and "rightOn" parameters.
    // The join condition is based on the equality
    // of the array of columns in the specified order.
    // Index left-hand dataframe
    const leftDfIndex = _getIndex(leftDf, (leftOn ? leftOn : on));
    // Perform join using right dataframe index
    const rightDfJoinCols = rightOn ? rightOn : on;
    // Null row for non-matching rows
    // excluding common properties, as these are
    // overwritten when combining objects
    let nullRow = _getLeftNullRow(leftDf, rightDf);
    let outputRowArray = [];
    for (let rightRow of rightDf.rows) {
        let matchingRows = _lookupIndex(rightRow, rightDfJoinCols, leftDfIndex);
        if (!matchingRows) {
            outputRowArray.push({...nullRow, ...rightRow});
            continue;
        }
        if (!isAnti) {
            for (let matchedRow of matchingRows)
                outputRowArray.push({...matchedRow, ...rightRow});
        }
    }

    let outputColumns = outputRowArray.length ? Object.getOwnPropertyNames(outputRowArray[0]) : leftDf.columns.concat(rightDf.columns);
    return new DataFrame(outputRowArray, outputColumns);
}


function _indexedFullOuterJoin(leftDf, rightDf, on, leftOn, rightOn, isAnti=false) {
    // Joins two dataframes based on the array
    // of join columns defined either in the "on",
    // or both "leftOn" and "rightOn" parameters.
    // The join condition is based on the equality
    // of the array of columns in the specified order.
    // Index left-hand dataframe
    let dfLeftJoin = _indexedLeftJoin(leftDf, rightDf, on, leftOn, rightOn, false);
    let dfRightAntiJoin = _indexedRightJoin(leftDf, rightDf, on, leftOn, rightOn, true);
    return new DataFrame([...dfLeftJoin.rows, ...dfRightAntiJoin.rows], dfLeftJoin.columns);
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
    return group(df, ...groupFuncs);
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
    }
    return indexValue;
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
