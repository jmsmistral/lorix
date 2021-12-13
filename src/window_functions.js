import {
    sum as d3Sum,
    min as d3Min,
    max as d3Max,
    mean as d3Mean,
    median as d3Median,
    quantile as d3Quantile,
    variance as d3Variance,
    deviation as d3Deviation
} from 'd3-array';

import {
    unboundedPreceding,
    unboundedProceeding,
    currentRow
} from './window.js'


export function sum(col) {
    let sumFunc = ((v, i) => d3Sum(v, d => d[col]));
    sumFunc.columnPropName = col;
    sumFunc.funcName = "sum()";
    return sumFunc;
}

export function min(col) {
    let minFunc = ((v, i) => d3Min(v, d => d[col]));
    minFunc.columnPropName = col;
    minFunc.funcName = "min()";
    return minFunc;
}

export function max(col) {
    let maxFunc = ((v, i) => d3Max(v, d => d[col]));
    maxFunc.columnPropName = col;
    maxFunc.funcName = "max()";
    return maxFunc;
}

export function mean(col) {
    let meanFunc = ((v, i) => d3Mean(v, d => d[col]));
    meanFunc.columnPropName = col;
    meanFunc.funcName = "mean()";
    return meanFunc;
}

export function median(col) {
    let medianFunc = ((v, i) => d3Median(v, d => d[col]));
    medianFunc.columnPropName = col;
    medianFunc.funcName = "median()";
    return medianFunc;
}

export function quantile(col, p=0.5) {
    let quantileFunc = ((v, i) => d3Quantile(v, p, d => d[col]));
    quantileFunc.columnPropName = col;
    quantileFunc.funcName = "quantile()";
    return quantileFunc;
}

export function variance(col) {
    let varianceFunc = ((v, i) => v.length > 1 ? d3Variance(v, d => d[col]) : null);
    varianceFunc.columnPropName = col;
    varianceFunc.funcName = "variance()";
    return varianceFunc;
}

export function stddev(col) {
    let stddevFunc = ((v, i) => v.length > 1 ? d3Deviation(v, d => d[col]) : null);
    stddevFunc.columnPropName = col;
    stddevFunc.funcName = "stddev()";
    return stddevFunc;
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
