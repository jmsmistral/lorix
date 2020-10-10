import fs from 'fs';

import d3Dsv from 'd3-dsv';

import { DataFrame } from './dataframe.js';


function readFile(filePath) {
    return new Promise((resolve, reject) => {
        fs.readFile(filePath, 'utf8', (err, data) => {
            // TODO provide descriptive error messages
            if (err) {
                reject(err);
                return;
            }
            resolve(data);
        });
    });
}

export async function readCsv(filePath) {
    const fileData = await readFile(filePath);
    return new Promise((resolve, reject) => {
        const rowArray = d3Dsv.csvParse(fileData, d3Dsv.autoType);
        const columns = Array.from(rowArray.columns);
        delete rowArray.columns;
        resolve(new DataFrame(rowArray, columns));
    });
};
