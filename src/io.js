import fs from "fs";
import path from "path";

import {
    autoType,
    csvParse,
    tsvParse,
    csvFormat,
    tsvFormat,
    dsvFormat
} from "d3-dsv";

import { DataFrame } from "./dataframe.js";
import { _isString } from "./utils.js";


function readFile(filePath) {
    return new Promise((resolve, reject) => {
        fs.readFile(filePath, "utf8", (err, data) => {
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
        const rowArray = csvParse(fileData, autoType);
        const columns = Array.from(rowArray.columns);
        delete rowArray.columns;
        resolve(new DataFrame(rowArray, columns));
    });
};

export async function readTsv(filePath) {
    const fileData = await readFile(filePath);
    return new Promise((resolve, reject) => {
        const rowArray = tsvParse(fileData, autoType);
        const columns = Array.from(rowArray.columns);
        delete rowArray.columns;
        resolve(new DataFrame(rowArray, columns));
    });
};

export async function readDsv(filePath, delimiter) {
    if (!delimiter || !_isString(delimiter) || delimiter == "") {
        throw Error("Incorrect delimiter passed to readDsv().");
    }
    const fileData = await readFile(filePath);
    return new Promise((resolve, reject) => {
        const rowArray = dsvFormat(delimiter).parse(fileData, autoType);
        const columns = Array.from(rowArray.columns);
        delete rowArray.columns;
        resolve(new DataFrame(rowArray, columns));
    });
};

export async function writeCsv(df, filePath) {
    await writeFile(filePath, csvFormat(df.rows));
};

export async function writeTsv(df, filePath) {
    await writeFile(filePath, tsvFormat(df.rows));
};

export async function writeDsv(df, filePath, delimiter) {
    if (!delimiter || !_isString(delimiter) || delimiter == "") {
        throw Error("Incorrect delimiter passed to writeDsv().");
    }
    await writeFile(filePath, dsvFormat(delimiter).format(df.rows));
};

export async function writeJson(df, filePath) {
    await writeFile(filePath, JSON.stringify(df.rows));
};
