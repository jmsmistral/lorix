import d3Array from 'd3-array';

import {
    unboundedPreceding,
    unboundedProceeding,
    currentRow
} from './window.js'


export function sum(col) {
    let sumFunc = ((v, i) => d3Array.sum(v, d => d[col]));
    sumFunc.columnPropName = col;
    sumFunc.funcName = "sum()";
    return sumFunc;

    // return ((v, i) => d3Array.sum(v, d => d[col]));
}

export function min(col) {
    let minFunc = ((v, i) => d3Array.min(v, d => d[col]));
    minFunc.columnPropName = col;
    minFunc.funcName = "min()";
    return minFunc;

    // return ((v, i) => d3Array.min(v, d => d[col]));
}

export function max(col) {
    let maxFunc = ((v, i) => d3Array.max(v, d => d[col]));
    maxFunc.columnPropName = col;
    maxFunc.funcName = "max()";
    return maxFunc;

    // return ((v, i) => d3Array.max(v, d => d[col]));
}

export function mean(col) {
    let meanFunc = ((v, i) => d3Array.mean(v, d => d[col]));
    meanFunc.columnPropName = col;
    meanFunc.funcName = "mean()";
    return meanFunc;

    // return ((v, i) => d3Array.mean(v, d => d[col]));
}

export function median(col) {
    let medianFunc = ((v, i) => d3Array.median(v, d => d[col]));
    medianFunc.columnPropName = col;
    medianFunc.funcName = "median()";
    return medianFunc;

    // return ((v, i) => d3Array.median(v, d => d[col]));
}

export function quantile(col, p=0.5) {
    let quantileFunc = ((v, i) => d3Array.quantile(v, p, d => d[col]));
    quantileFunc.columnPropName = col;
    quantileFunc.funcName = "quantile()";
    return quantileFunc;

    // return ((v, i) => d3Array.quantile(v, p, d => d[col]));
}

export function variance(col) {
    let varianceFunc = ((v, i) => v.length > 1 ? d3Array.variance(v, d => d[col]) : null);
    varianceFunc.columnPropName = col;
    varianceFunc.funcName = "variance()";
    return varianceFunc;

    // return ((v, i) => v.length > 1 ? d3Array.variance(v, d => d[col]) : null);
}

export function stddev(col) {
    let stddevFunc = ((v, i) => v.length > 1 ? d3Array.deviation(v, d => d[col]) : null);
    stddevFunc.columnPropName = col;
    stddevFunc.funcName = "stddev()";
    return stddevFunc;

    // return ((v, i) => v.length > 1 ? d3Array.deviation(v, d => d[col]) : null);
}

export function lag(col, n) {
    let lagFunc = (v, i) => v.length > 1 ? v[0][col] : null;
    lagFunc.setWindowSize = true;
    lagFunc.windowSize = [n, currentRow];
    lagFunc.columnPropName = col;
    lagFunc.funcName = "lag()";
    return lagFunc;
}

export function lead(col, n) {
    let leadFunc = (v, i) => v.length > 1 ? v[v.length - 1][col] : null;
    leadFunc.setWindowSize = true;
    leadFunc.windowSize = [currentRow, n];
    leadFunc.columnPropName = col;
    leadFunc.funcName = "lead()";
    return leadFunc;
}

export function rownumber() {
    if (arguments.length)
        throw Error("Window function 'rownumber()' takes no arguments.");

    let rownumberFunc = (v, i) => i + 1;
    rownumberFunc.setWindowSize = true;
    rownumberFunc.windowSize = [unboundedPreceding, unboundedProceeding];
    rownumberFunc.columnPropName = undefined;
    rownumberFunc.funcName = "rownumber()";
    return rownumberFunc;
}
