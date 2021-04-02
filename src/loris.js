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
    median
} from "./groups.js";
import { DataFrame } from "./dataframe.js";


// loris library definition
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

    // DataFrame class
    DataFrame: DataFrame
};
