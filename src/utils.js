import { DummyEqualDataFrame, DummyNonEqualDataFrame } from './proxy.js';

import lodash from 'lodash';


function _getRandomString() {
    return Math.random().toString(36).substring(2, 15);
}

export function generateRandomValue() {
    return (_getRandomString() + _getRandomString());
}

export function validateJoinFunctionReferencesWithProxy(fn, leftDfCols, rightDfCols) {
    // Runs join condition function on dummy DFs to
    // check which columns are being referenced.
    let leftNonEqualDummyDf = new DummyNonEqualDataFrame(leftDfCols);
    let rightNonEqualDummyDf = new DummyNonEqualDataFrame(rightDfCols);
    fn(leftNonEqualDummyDf.rows[0], rightNonEqualDummyDf.rows[0]);

    let leftEqualDummyDf = new DummyEqualDataFrame(leftDfCols);
    let rightEqualDummyDf = new DummyEqualDataFrame(rightDfCols);
    fn(leftEqualDummyDf.rows[0], rightEqualDummyDf.rows[0]);

    return {
        "left": lodash.union([
            ...leftNonEqualDummyDf.getAccessedColumns(),
            ...leftEqualDummyDf.getAccessedColumns()
        ]),
        "right": lodash.union([
            ...rightNonEqualDummyDf.getAccessedColumns(),
            ...rightEqualDummyDf.getAccessedColumns()
        ]),
        "both": lodash.union([
            ...leftNonEqualDummyDf.getAccessedColumns(),
            ...leftEqualDummyDf.getAccessedColumns(),
            ...rightNonEqualDummyDf.getAccessedColumns(),
            ...rightEqualDummyDf.getAccessedColumns()
        ]),
    };
}

export function validateFunctionReferencesWithProxy(fn, cols) {
    // Runs join condition function on dummy DFs to
    // check which columns are being referenced.
    let nonEqualDummyDf = new DummyNonEqualDataFrame(cols);
    fn(nonEqualDummyDf.rows[0]);

    let equalDummyDf = new DummyEqualDataFrame(cols);
    fn(equalDummyDf.rows[0]);

    return lodash.union([
        nonEqualDummyDf,
        equalDummyDf
    ]);
}

export function validateJoinArrayReferences(joinCols, dfCols) {
    // Returns true if all columns in `cols` are in `expectedCols`.
    return joinCols.filter(col => !(dfCols.includes(col))).length == 0;
}

export function getInvalidJoinColumns(leftDfCols, rightDfCols, leftCompareCols, rightCompareCols) {
    // Returns the unique array of columns that
    // are referenced but do not exist in the DataFrame.
    if (arguments.length == 3) {
        // let cols = leftCompareCols;
        // return (
        //     lodash.union(
        //         lodash.difference(cols, leftDfCols),
        //         lodash.difference(cols, rightDfCols)
        //     )
        // );
        rightCompareCols = leftCompareCols;
    }
    return (
        lodash.union(
            lodash.difference(leftCompareCols, leftDfCols),
            lodash.difference(rightCompareCols, rightDfCols)
        )
    );

}

export function validateOverlappingColumns(leftDfCols, rightDfCols, leftJoinCols, rightJoinCols) {
    // Check if any of the columns that are not
    // part of the join condition, have the same
    // name between both datasets. Throw an error
    // if there is any overlap.
    // The point is to avoid overwriting data when
    // joining, for columns with the same name.
    if (arguments.length == 3) {
        rightJoinCols = leftJoinCols;
    }
    let leftDfFilteredCols = lodash.difference(leftDfCols, leftJoinCols);
    let rightDfFilteredCols = lodash.difference(rightDfCols, rightJoinCols);
    let overlappingCols = lodash.intersection(leftDfFilteredCols, rightDfFilteredCols);
    if (overlappingCols.length) {
        throw Error(`Overlapping columns with the same name: ${overlappingCols}`);
    }
}

export function validateOverlappingColumnsInFunction(leftDfCols, rightDfCols, leftJoinCols, rightJoinCols) {
    // Check if any of the columns that are not
    // part of the functional join condition, have
    // the same name between both datasets. Throw
    // an error if there is any overlap.
    // The point is to avoid overwriting data when
    // joining, for columns with the same name.
    let commonJoinCols = lodash.intersection(leftJoinCols, rightJoinCols);
    let leftDfFilteredCols = lodash.difference(leftDfCols, commonJoinCols);
    let rightDfFilteredCols = lodash.difference(rightDfCols, commonJoinCols);
    let overlappingCols = lodash.intersection(leftDfFilteredCols, rightDfFilteredCols);
    if (overlappingCols.length) {
        throw Error(`Overlapping columns with the same name: ${overlappingCols}`);
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
