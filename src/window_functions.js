import d3Array from 'd3-array';

import {
    unboundedPreceding,
    unboundedProceeding,
    currentRow
} from './window.js'


export function sum(col) {
    return ((v, i) => d3Array.sum(v, d => d[col]));
}

export function min(col) {
    return ((v, i) => d3Array.min(v, d => d[col]));
}

export function max(col) {
    return ((v, i) => d3Array.max(v, d => d[col]));
}

export function mean(col) {
    return ((v, i) => d3Array.mean(v, d => d[col]));
}

export function median(col) {
    return ((v, i) => d3Array.median(v, d => d[col]));
}

export function quantile(col, p=0.5) {
    return ((v, i) => d3Array.quantile(v, p, d => d[col]));
}

export function variance(col) {
    return ((v, i) => v.length > 1 ? d3Array.variance(v, d => d[col]) : null);
}

export function stddev(col) {
    return ((v, i) => v.length > 1 ? d3Array.deviation(v, d => d[col]) : null);
}

export function lag(col, n) {
    let lagFunc = (v, i) => v.length > 1 ? v[0][col] : null;
    lagFunc.setWindowSize = true;
    lagFunc.windowSize = [n, currentRow];
    return lagFunc;
}

export function lead(col, n) {
    let leadFunc = (v, i) => v.length > 1 ? v[v.length - 1][col] : null;
    leadFunc.setWindowSize = true;
    leadFunc.windowSize = [currentRow, n];
    return leadFunc;
}

export function rownumber() {
    if (arguments.length)
        throw Error("Window function 'rownumber()' takes no arguments.");

    let rownumberFunc = (v, i) => i + 1;
    rownumberFunc.setWindowSize = true;
    rownumberFunc.windowSize = [unboundedPreceding, unboundedProceeding];
    return rownumberFunc;
}
