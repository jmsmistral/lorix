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
    unboundedPreceding,
    unboundedProceeding,
    currentRow,

    window
} from "./src/window.js";

import {
    mean,
    median,
    quantile,
    variance,
    stddev,
    min,
    max,
    sum,
    lag,
    lead,
    rownumber
} from "./src/window_functions.js";

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
    quantile: quantile,
    variance: variance,
    stddev: stddev,
    min: min,
    max: max,
    sum: sum,
    lag: lag,
    lead: lead,
    rownumber: rownumber,

    unboundedPreceding: unboundedPreceding,
    unboundedProceding: unboundedProceeding,
    currentRow: currentRow,

    window: window,

    // DataFrame class
    DataFrame: DataFrame
};
