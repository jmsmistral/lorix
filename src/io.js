import fs from 'fs';
import path from "path";
import { fileURLToPath } from 'url';

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

function writeFile(filePath, string) {
    return new Promise((resolve, reject) => {
        console.log(`Writing file ${path.join(process.cwd(), filePath)}`)
        fs.writeFile(path.join(process.cwd(), filePath), string, "utf8", (err) => {
            // TODO provide descriptive error messages
            if (err) {
                reject(err);
                return;
            }
            resolve();
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

export async function readTsv(filePath) {
    const fileData = await readFile(filePath);
    return new Promise((resolve, reject) => {
        const rowArray = d3Dsv.tsvParse(fileData, d3Dsv.autoType);
        const columns = Array.from(rowArray.columns);
        delete rowArray.columns;
        resolve(new DataFrame(rowArray, columns));
    });
};

export async function writeTsv(df, filePath) {
    await writeFile(filePath, d3Dsv.tsvFormat(df.rows));
};
