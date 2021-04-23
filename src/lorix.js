import {
    readCsv,
    readTsv,
    readDsv,
    writeCsv,
    writeTsv,
    writeDsv,
    writeJson
} from "./io.js";
import {
    median,
    quantile,
    variance,
    stdev,
    min,
    max
} from "./groups.js";
import { DataFrame } from "./dataframe.js";


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
    median: median,
    quantile: quantile,
    variance: variance,
    stdev: stdev,
    min: min,
    max: max,

    // DataFrame class
    DataFrame: DataFrame
};
