import { DummyEqualDataFrame, DummyNonEqualDataFrame } from './proxy.js';

import lodash from 'lodash';


function _getRandomString() {
    return Math.random().toString(36).substring(2, 15);
}

export function generateRandomValue() {
    return (_getRandomString() + _getRandomString());
}

export function validateJoinFunctionReferencesWithProxy(fn, leftDfCols, rightDfCols, dfColsToReturn = "left") {
    // Runs join condition function on dummy DFs to
    // check which columns are being referenced.
    let leftNonEqualDummyDf = new DummyNonEqualDataFrame(leftDfCols);
    let rightNonEqualDummyDf = new DummyNonEqualDataFrame(rightDfCols);
    fn(leftNonEqualDummyDf.rows[0], rightNonEqualDummyDf.rows[0]);

    let leftEqualDummyDf = new DummyEqualDataFrame(leftDfCols);
    let rightEqualDummyDf = new DummyEqualDataFrame(rightDfCols);
    fn(leftEqualDummyDf.rows[0], rightEqualDummyDf.rows[0]);

    if (dfColsToReturn == "right") {
        return lodash.union([
            rightNonEqualDummyDf,
            rightEqualDummyDf
        ]);
    }
    return lodash.union([
        leftNonEqualDummyDf,
        leftEqualDummyDf
    ]);
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
