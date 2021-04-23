import lorix from '../lorix.js';


// loading data data
let df1 = await lorix.readCsv("./data/test.csv");
let df2 = await lorix.readCsv('./data/test2.csv');
// let df3 = await lorix.readDsv('test.psv'); // Error
// let df3 = await lorix.readDsv('test.psv', ""); // Error
// let df3 = await lorix.readDsv('test.psv', {}); // Error
// let df3 = await lorix.readDsv('test.psv', "|");

// selecting columns
// console.log(df);
// const df1 = df.select('id', 'name')
// console.log(df1);
// const df2 = df1.select('id');
// console.log(df2);
// df.head();

// dropping columns
// let df3 = df.drop('name');

console.log("df1");
df1.head();

console.log("df2");
df2.head();
// df3.head();

// array iteration
// console.log(df1.columns);
// console.log([...df1]);
// console.log(df1.toArray());

console.log("df1.withColumn()");
// df1.withColumn("newCol", df1.col("id") + df1.col("age")); // Attempt at cleaner syntax
// df1 = df1.withColumn("newCol", (row) => row["id"] + row["age"]);
// df1 = df1.withColumn("newCol", () => 1 + 2);
// df1 = df1.withColumn("newCol", () => new Date());
// df1 = df1.withColumn("newCol", (row) => row["somerandomprop"]); // Results in column with undefined values
// df1 = df1.withColumn("1newCol", (row) => row["id"] + row["age"]); // Error - invalid column reference
// df1.head();
df1 = (
    df1
    .withColumn("newCol", () => 1)
    .withColumn("newCol2", () => 2)
)

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

console.log("df1.window()");
df1.head();
df1.window(
    ["id"],
    [["age"], ["desc"]],
    {
        "min": lorix.min("age"),
        "max": lorix.max("age"),
        "median": lorix.median("age"),
        "quantile": lorix.quantile("age"), // Default is 0.5 (median)
        "first_qrtl": lorix.quantile("age", 0.25),  // First quartile
        "third_qrtl": lorix.quantile("age", 0.75),  // Third quartile
        "variance": lorix.variance("age"),
        "stdev": lorix.stdev("age")
    }
).head();

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
// (df1.innerJoin(df2, (l, r) => l.id == r.id, false)).head(); // Error - number of arguments
// (df1.innerJoin(df2, "id")).head(); // Error - argument types
// (df1.innerJoin(df2, (l, r) => l.id == r.id)).head();  // Non-indexed inner join
// (df1.innerJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head(); // Non-indexed inner join
// (df1.innerJoin(df2, ["id"])).head();  // Indexed inner join
// (df1.innerJoin(df2, ["id", "age"])).head();  // Indexed inner join

console.log('left join');
// (df1.leftJoin(df2, "id")).head(); // Error - argument types
// (df1.leftJoin(df2, (l, r) => l.id == r.id)).head(); // Non-indexed left join
// (df1.leftJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head(); // Non-indexed left join
// (df1.leftJoin(df2, (l, r) => (l.id == r.id) | (l.age == r.age))).head();
// (df1.leftJoin(df2, ["id"])).head();
// (df1.leftJoin(df2, ["id", "age"], ["id", "age"])).head();
// df1.leftJoin(df2); // Error

console.log('right join');
// (df1.rightJoin(df2, (l, r) => l.id == r.id)).head();
// (df1.rightJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head();
// (df1.rightJoin(df2, (l, r) => (l.id == r.id) | (l.age == r.age))).head();
// (df1.rightJoin(df2, ["id"])).head();
// (df1.rightJoin(df2, ["id", "age"], ["id", "age"])).head();
// df1.rightJoin(df2); // Error

// try {
//     df1.createIndex("id");
//     console.log(df1.isColumnIndexed("id"));
//     // console.log(df1.isColumnIndexed("names")); // Error
//     df1.createIndex("name");
//     // df1.createIndex("names"); // Error
// } catch(err) {
//     console.log(err);
// }

await lorix.writeTsv(df1, "df1_output.tsv");
await lorix.writeCsv(df1, "df1_output.csv");
// await lorix.writeDsv(df1, "df1_output.psv"); // Error
// await lorix.writeDsv(df1, "df1_output.psv", ""); // Error
// await lorix.writeDsv(df1, "df1_output.psv", {}); // Error
await lorix.writeDsv(df1, "df1_output.psv", "|");
await lorix.writeJson(df1, "df1_output.json");
