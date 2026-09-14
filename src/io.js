import { readFile, writeFile } from 'node:fs/promises';
import {
  autoType,
  csvParse,
  tsvParse,
  csvFormat,
  tsvFormat,
  dsvFormat,
} from 'd3-dsv';
import { DataFrame } from './dataframe.js';

function parseData(text, parse) {
  const rows = parse(text, autoType);
  const columns = [...rows.columns];
  delete rows.columns;
  return new DataFrame(rows, columns);
}

function delimiterFormat(delimiter) {
  if (
    typeof delimiter !== 'string' ||
    delimiter.length !== 1 ||
    /[\r\n"]/.test(delimiter)
  ) {
    throw Error(
      'Delimiter must be a single character other than a quote or newline.',
    );
  }
  return dsvFormat(delimiter);
}

function validateFrame(df) {
  if (!(df instanceof DataFrame)) throw Error('Export requires a DataFrame.');
}

export async function readCsv(filePath) {
  return parseData(await readFile(filePath, 'utf8'), csvParse);
}

export async function readTsv(filePath) {
  return parseData(await readFile(filePath, 'utf8'), tsvParse);
}

export async function readDsv(filePath, delimiter) {
  const format = delimiterFormat(delimiter);
  return parseData(await readFile(filePath, 'utf8'), format.parse);
}

export async function writeCsv(df, filePath) {
  validateFrame(df);
  await writeFile(filePath, csvFormat(df.rows, df.columns), 'utf8');
}

export async function writeTsv(df, filePath) {
  validateFrame(df);
  await writeFile(filePath, tsvFormat(df.rows, df.columns), 'utf8');
}

export async function writeDsv(df, filePath, delimiter) {
  validateFrame(df);
  await writeFile(
    filePath,
    delimiterFormat(delimiter).format(df.rows, df.columns),
    'utf8',
  );
}

export async function writeJson(df, filePath) {
  validateFrame(df);
  await writeFile(filePath, JSON.stringify(df.rows), 'utf8');
}
