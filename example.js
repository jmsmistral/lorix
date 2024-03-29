import lodash from 'lodash';
import lorix from './lorix.js';
import { currentRow } from './src/window.js';

import {
    verySmallDataFrame1,
    verySmallDataFrame2,
    verySmallDataFrame3,
    verySmallDataFrame4,
    verySmallValidObjArray,
    verySmallInvalidObjArray,
    verySmallDataFrameInnerJoinResult,
    smallDataFrame1,
    smallDataFrame2,
    smallDataFrame3,
    smallDataFrame4,
    smallDataFrame5,
    smallDataFrame6,
    iris
} from './test/sample_data.js'


// loading data data
// let df1 = await lorix.readCsv("./test/data/iris.csv");
// let df2 = await lorix.readCsv('./data/test2.csv');
// let df3 = await lorix.readDsv('test.psv'); // Error
// let df3 = await lorix.readDsv('test.psv', ""); // Error
// let df3 = await lorix.readDsv('test.psv', {}); // Error
// let df3 = await lorix.readDsv('test.psv', "|");

let vsDf1 = verySmallDataFrame1;
let vsDf2 = verySmallDataFrame2;
let vsDf3 = verySmallDataFrame3;
let vsDf4 = verySmallDataFrame4;

let sDf1 = smallDataFrame1;
let sDf2 = smallDataFrame2;
let sDf3 = smallDataFrame3;
let sDf4 = smallDataFrame4;
let sDf5 = smallDataFrame5;
let sDf6 = smallDataFrame6;

let irisDf = iris;

// let arr1 = verySmallValidObjArray;
// let arr2 = verySmallInvalidObjArray;

console.log("lorix.DataFrame.fromArray()");
// console.log("valid data");
// let df3 = lorix.DataFrame.fromArray(arr1);
// df3.head();
// console.log("invalid data");
// let df4 = lorix.DataFrame.fromArray(arr2);
// df4.head();

// selecting columns
console.log("df.select()");
// console.log(df);
// const df1 = df.select('id', 'name')
// console.log(df1);
// const df2 = df1.select('id');
// console.log(df2);
// df.head();

// dropping columns
console.log("df.drop()");
// let df3 = df.drop('name');

// head
console.log("df.head()");
// console.log("df1");
// df1.head();

// console.log("df2");
// df2.head();

// console.log("df3");
// df3.head();

// console.log("sDf4");
// sDf4.head();

// console.log("sDf5");
// sDf5.head();

// console.log("df5s");
// df5.head();

// console.log("df6s");
// df6.head();

// console.log("iris");
// irisDf.head();


// sort data
console.log("df.orderBy()");
// df3.orderBy(["invalidColumn"]).head();
// console.log(df3.orderBy(["id"]).toArray());
// console.log(df3.orderBy(["name"]).toArray());
// console.log(df3.orderBy(["id", "weight"]).toArray());
// console.log(df3.orderBy(["id", "weight"], ["desc", "asc"]).toArray());
// console.log(df3.orderBy(["id"], "test").toArray());
// console.log(df3.orderBy(["id"], ["wog"]).toArray());

// array iteration
// console.log(df1.columns);
// console.log([...df1]);
// console.log(df1.toArray());

console.log("df.withColumn()");
// df1 = df1.withColumn("newCol");
// df1.withColumn("newCol", df1.col("id") + df1.col("age")); // Attempt at cleaner syntax
// df1 = df1.withColumn("newCol", (row) => row["id"] + row["age"]);
// df1 = df1.withColumn("newCol", () => 1 + 2);
// df1 = df1.withColumn("newCol", () => new Date());
// df1 = df1.withColumn("newCol", (row) => row["somerandomprop"]); // Error - reference to non-ex
// df1 = df1.withColumn("1newCol", (row) => row["id"] + row["age"]); // Error - invalid column name
// df1.head();
// df2 = (
//     df2
//     .withColumn("newCol", () => 1)
//     .withColumn("newCol1", () => 2)
// )

// Error - needs a hotfix because expr is run on a dummy proxy dataframe
// sDf5.withColumn("new_date", (row) => row["latest_date"].toISOString()).head();

console.log("df1.filter()");
// df3.filter((r) => r["id"] == 100).head();
// (
    //     df3
    //     .filter((r) => r["weight"] < 80)
    //     .filter((r) => r["id"] > 20)
    //     .head()
    // )
// df3.filter((r) => r["nonExistantCol"] == 100).head(); // Error - invalid column
// df3.filter("test").head(); // Error - needs a function

// Error - needs a hotfix because expr is run on a dummy proxy dataframe
// sDf5.filter((row) => row["latest_date"].toISOString().split("T")[0] == "2023-02-06").head();

console.log("df.distinct()");
// (
//     df4
//     .select("id")
//     .distinct()
//     .filter((r) => r["id"] > 50)
//     .orderBy(["id"], ["desc"])
//     .head(df4.rows.length)
// )


console.log("df1.groupBy()");
// console.log(df1.groupBy(["id", "age"]));
// df1.groupBy(["id", "age"], {"newCol": "sum"});
// df1.groupBy(["id", "age"], {"newCol": "mean"});
// df1.groupBy(["id", "age"], {"newCol": "count"});
// df1.groupBy(
//     ["id", "age"],
//     {
//         "newCol": ["sum", "mean", "count"],
//         "newCol2": "sum"
//     }
// ).head();
// df1.groupBy(["id", "age"], {"newColz": "count"}); // Error - invalid column reference

// console.log(
//     irisDf
//     .groupBy(
//         ["species"],
//         {
//             "sepal_length": ["min", "max", "mean", "count", "sum"]
//         }
//     )
//     .toArray()
// );

// (
//     irisDf
//     .groupBy(
//         ["species"],
//         {
//             "sepal_length": ["min", "max", "mean", "count", "sum"]
//         }
//     )
//     .head()
// )


console.log("df.window()");
// (
//     df5
//     .withColumn("sum", lorix.window(lorix.sum("salary"), ["dept"], [["salary"], ["desc"]]))
//     .withColumn("lag", lorix.window(lorix.lag("salary", 1), ["dept"], [["salary"], ["desc"]]))
//     .withColumn("lead", lorix.window(lorix.lead("salary", 1), ["dept"], [["salary"], ["desc"]]))
//     .withColumn("stddev", lorix.window(lorix.stddev("salary"), ["dept"], [["salary"], ["desc"]]))
//     .withColumn("rownum", lorix.window(lorix.rownumber(), ["dept"], [["salary"], ["desc"]]))
//     // .withColumn("sum", lorix.window(lorix.sum("salary"), ["dept"], [], [1, lorix.currentRow]))
//     // .withColumn("sum", lorix.window(lorix.sum("salary"), ["dept"], [], [lorix.unboundedPreceding, lorix.unboundedProceding]))
//     .withColumn("stddev", (r) => Math.round(r["stddev"], 0))
//     // .orderBy(["dept", "salary"], ["asc", "desc"])
//     // .filter((r) => r["row"] == 1)
//     .head(100)
// );


// (
    // df5
    // .withColumn("sum", lorix.window(lorix.sum("caca")))
    // .withColumn("sum", lorix.window(lorix.sum("salary"),          ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("lag", lorix.window(lorix.lag("salary", 1),       ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("lead", lorix.window(lorix.lead("salary", 1),     ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("stddev", lorix.window(lorix.stddev("salary"),    ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("variance", lorix.window(lorix.variance("salary"),["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("mean", lorix.window(lorix.mean("salary"),        ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("median", lorix.window(lorix.median("salary"),    ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("min", lorix.window(lorix.min("salary"),          ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("max", lorix.window(lorix.max("salary"),          ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("rownum", lorix.window(lorix.rownumber(),         ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
    // .withColumn("stddev", (r) => r["stddev"] !== null ? Math.round(r["stddev"], 0) : null)
    // .withColumn("variance", (r) => r["variance"] !== null ? Math.round(r["variance"], 0) : null)
    // .withColumn("mean", (r) => r["mean"] !== null ? Math.round(r["mean"], 0) : null)
    // .withColumn("median", (r) => r["median"] !== null ? Math.round(r["median"], 0) : null)
    // .head(100)
// )

// (
//     df5
//     .withColumn("mean", lorix.window(lorix.mean("salary"), ["dept"], []))
//     .withColumn("mean", (r) => Math.round(r["mean"], 0))
//     .orderBy(["dept", "salary"], ["asc", "desc"])
//     .withColumn("row_num", lorix.window(lorix.rowNumber(), ["dept"], ["salary"]))
//     .head(100)
// );

// console.log(df3.withColumn("quantile", lorix.window(lorix.quantile("age"), ["id"], [["weight"], ["desc"]])));
// df3.withColumn("quantile", lorix.window(lorix.quantile("age"), ["id"], [["weight"], ["desc"]]));
// df5.withColumn("mean", lorix.window(lorix.mean("salary"), [], [])).head(100);
// df3.withColumn("max", lorix.window(lorix.max("weight"), ["id"], [["weight"], ["desc"]])).head(100);
// df3.withColumn("median", lorix.window(lorix.median("weight"), ["id"], [["weight"], ["desc"]])).head(100);
// df3.withColumn("quantile", lorix.window(lorix.quantile("weight"), ["id"], [["weight"], ["desc"]])).head(100);
// df3.withColumn("firstQuartile", lorix.window(lorix.quantile("weight", 0.25), ["id"], [["weight"], ["desc"]])).head(100);
// df3.withColumn("thirdQuartile", lorix.window(lorix.quantile("weight", 0.75), ["id"], [["weight"], ["desc"]])).head(100);
// df3.withColumn("variance", lorix.window(lorix.variance("weight"), ["id"], [["weight"], ["desc"]])).head(100);
// df3.withColumn("stdev", lorix.window(lorix.stdev("weight"), ["id"], [["weight"], ["desc"]])).head(100);

// df.withColumn("quantile", lorix.window(lorix.sum("age"), ["id"], [], [lorix.unboundedPreceeding, lorix.currentRow]));

// df1.head();
// df1.window(
//     ["id"],
//     [["age"], ["desc"]],
//     {
//         "min": lorix.min("age"),
//         "max": lorix.max("age"),
//         "median": lorix.median("age"),
//         "quantile": lorix.quantile("age"), // Default is 0.5 (median)
//         "first_qrtl": lorix.quantile("age", 0.25),  // First quartile
//         "third_qrtl": lorix.quantile("age", 0.75),  // Third quartile
//         "variance": lorix.variance("age"),
//         "stdev": lorix.stdev("age")
//     }
// ).head();

console.log("df1.orderBy()");
// df1.orderBy(["age"]).head();
// df1.orderBy(["id", "age"], ["asc", "desc"]).head();
// df1.orderBy("id").head(); // Error - need an array of columns


console.log("df1 for...of iteration");
// for (let row of df1) {
//     console.log(row);
// }

// joins
console.log('cross join');
// (df1.crossJoin(df2, (l, r) => l.id == r.id)).head(); // Error - number of arguments
// df1.crossJoin(df2).head(); // Cross join

console.log('inner join');
// (df1.innerJoin(df2, ["id", "testicle", "testiculae"])).head();
// (df1.innerJoin(df2, ["id"])).head();
// df1 = (
//     df1
//     .withColumn("name", (row) => {if (row.id == 3) { return "gary" } else return row.name })
// )
// df1.head();
// (df1.innerJoin(df2, (l, r) => (l.id == r.id) || (r.name == "gary") )).head();
// (df1.innerJoin(df2, (l, r) => (l.id == r.id) )).head();

// console.log((df1.innerJoin(df2, ["id"], ["id"])).toArray());
// console.log(verySmallDataFrameInnerJoinResult.toArray());

// console.log((df1.innerJoin(df2, ["id"])).toArray());
// (df1.innerJoin(df2, (l, r) => l.id == r.id)).head();
// (df1.innerJoin(df2, (l, r) => (l.id == r.id) & (l.name == r.name))).head();
// (df1.innerJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head(); // Error - reference to non-existent column


// (df1.innerJoin(df2, (l, r) => l.id == r.id, false)).head(); // Error - number of arguments
// (df1.innerJoin(df2, "id")).head(); // Error - argument types
// (df1.innerJoin(df2, (l, r) => l.id == r.id)).head();  // Non-indexed inner join
// (df1.innerJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head(); // Non-indexed inner join
// (df1.innerJoin(df2, ["id"])).head();  // Indexed inner join
// (df1.innerJoin(df2, ["id", "age"])).head();  // Indexed inner join

console.log('left join');
// (df1.leftJoin(df2, (l, r) => l.id == r.id)).head(); // Non-indexed left join

// (df1.leftJoin(df2, "id")).head(); // Error - argument types
// (df1.leftJoin(df2, (l, r) => l.id == r.id)).head(); // Non-indexed left join
// (df1.leftJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head(); // Non-indexed left join
// (df1.leftJoin(df2, (l, r) => (l.id == r.id) | (l.age == r.age))).head();
// (df1.leftJoin(df2, ["id"])).head();
// (df1.leftJoin(df2, ["id", "age"], ["id", "age"])).head();
// df1.leftJoin(df2); // Error

console.log('right join');
// (df1.rightJoin(df2, (l, r) => l.id == r.id)).head();

// (df1.rightJoin(df2, (l, r) => l.id == r.id)).head();
// (df1.rightJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head();
// (df1.rightJoin(df2, (l, r) => (l.id == r.id) | (l.age == r.age))).head();
// (df1.rightJoin(df2, ["id"])).head();
// (df1.rightJoin(df2, ["id", "age"], ["id", "age"])).head();
// df1.rightJoin(df2); // Error

console.log('left anti join');
// console.log((df1.leftAntiJoin(df2, (l, r) => (l.id == r.id) && (l.name == r.name)  )).toArray()) // Non-indexed left join
// console.log(df1.rightAntiJoin(df2, (l, r) => (l.id == r.id) && (l.name == r.name)  ).toArray()); // Non-indexed left join
// (df1.leftAntiJoin(df2, (l, r) => (l.id == r.id) && (l.name == r.name)  )).head(100); // Non-indexed left join
// (df1.rightAntiJoin(df2, (l, r) => (l.id == r.id) && (l.name == r.name)  )).head(100); // Non-indexed left join
// (df2.fullOuterJoin(df6, (l, r) => (l.id == r.id) && (l.name == r.name)  )).head(100); // Non-indexed left join
// (df2.fullOuterJoin(df6, ["id", "name"])).head(100);
// console.log((df2.fullOuterJoin(df6, ["id", "name"])).toArray());

// (df1.leftJoin(df2, "id")).head(); // Error - argument types
// (df1.leftJoin(df2, (l, r) => l.id == r.id)).head(); // Non-indexed left join
// (df1.leftJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head(); // Non-indexed left join
// (df1.leftJoin(df2, (l, r) => (l.id == r.id) | (l.age == r.age))).head();

// (df1.leftAntiJoin(df2, ["id", "name"])).head();
// (df1.rightAntiJoin(df2, ["id", "name"])).head();

// (df1.leftJoin(df2, ["id", "age"], ["id", "age"])).head();
// df1.leftJoin(df2); // Error

console.log("replace()");
// signature: df1.replace(["col"], valueToReplace, newValue);
// (df4.replace(["name"], "billy", "silly")).head(100);

// console.log(sDf4.regexReplace(["name"], /r/i, "rrr").toArray());
// console.log(sDf4.regexReplace(["name"], /r/ig, "rrr").toArray());
// console.log(sDf4.regexReplace(["name", "colour"], /r/ig, "rrr").toArray());

console.log("pivot()");
sDf6.head();
sDf6.pivot(
    ["id", "name"], // GroupBy Cols
    "colour",       // Pivot Col
    "weight",       // Value Col
    "count"           // Agg Type
).head();

sDf6.pivot(
    ["id", "name"], // GroupBy Cols
    "colour",       // Pivot Col
    "weight",       // Value Col
    "sum"           // Agg Type
).head();

sDf6.pivot(
    ["id", "name"], // GroupBy Cols
    "colour",       // Pivot Col
    "weight",       // Value Col
    "mean"           // Agg Type
).head();

console.log("unionByName()");
// console.log(sDf1.unionByName(sDf2).toArray());

// await lorix.writeTsv(df1, "df1_output.tsv");
// await lorix.writeCsv(df1, "df1_output.csv");
// await lorix.writeDsv(df1, "df1_output.psv"); // Error
// await lorix.writeDsv(df1, "df1_output.psv", ""); // Error
// await lorix.writeDsv(df1, "df1_output.psv", {}); // Error
// await lorix.writeDsv(df1, "df1_output.psv", "|");
// await lorix.writeJson(df1, "df1_output.json");
