import {
    readCsv,
    readTsv,
    readDsv,
    writeCsv,
    writeTsv,
    writeDsv,
    writeJson
} from "./src/io.js";

import {
    mean,
    median,
    mode,
    quantile,
    variance,
    stdev,
    min,
    max,
    sum,
    rowNumber,

    unboundedPreceding,
    unboundedProceding,
    currentRow,

    window
} from "./src/window.js";

import { DataFrame } from "./src/dataframe.js";


// lorix library definition
export default {
    // Import data
    readCsv: readCsv,
    readTsv: readTsv,
    readDsv: readDsv,

    // Export data
    writeCsv: writeCsv,
    writeTsv: writeTsv,
    writeDsv: writeDsv,
    writeDsv: writeDsv,
    writeJson: writeJson,

    // Window function aggregate types
    mean: mean,
    median: median,
    mode: mode,
    quantile: quantile,
    variance: variance,
    stdev: stdev,
    min: min,
    max: max,
    sum: sum,
    rowNumber: rowNumber,

    unboundedPreceding: unboundedPreceding,
    unboundedProceding: unboundedProceding,
    currentRow: currentRow,

    window: window,

    // DataFrame class
    DataFrame: DataFrame
};
