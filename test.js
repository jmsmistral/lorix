import loris from './src/loris.js';

// function test() {
//     readCsv('test.csv')
//         .then((data) => {
//             console.log(data);
//         });
// }
// test();

// async function test() {
//     const data = await readCsv('test.csv');
//     console.log(data);
// }
// test();
async function test() {

    // empty dataframe
    // let dfEmpty = new loris.DataFrame();
    // let dfEmpty = new loris.DataFrame([], []);
    // dfEmpty.head();
    // dfEmpty.select('id', 'age').head();  // Error
    // dfEmpty.drop('id', 'age').head();  // Error
    // let dfMalformed = new loris.DataFrame([]);
    // dfMalformed.head();
    // console.log(dfMalformed.columns);

    // let dataArr = [
    //     {"colr1c1": "valr1c1", "colr1c2": "valr1c2"},
    //     {"colr2c1": "valr2c1", "colr2c2": "valr2c2"},
    //     {"colr3c1": "valr3c1", "colr3c2": "valr3c2"}
    // ]; // Error
    // let dataArr = [
    //     {},
    //     {},
    //     {}
    // ]; // Error
    // let dataArr = [
    //     {"col1": "valr1c1", "col2": "valr1c2"},
    //     {"col1": "valr2c1", "col2": "valr2c2"},
    //     {"col1": "valr3c1", "col2": "valr3c2"}
    // ];

    // let df0 = loris.DataFrame.fromArray(dataArr);
    // console.log("df0");
    // df0.head();

    // loading data data
    let df1 = await loris.readCsv('test.csv');
    let df2 = await loris.readCsv('test2.csv');
    // let df3 = await loris.readDsv('test.psv'); // Error
    // let df3 = await loris.readDsv('test.psv', ""); // Error
    // let df3 = await loris.readDsv('test.psv', {}); // Error
    // let df3 = await loris.readDsv('test.psv', "|");

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

    console.log("df1.withColumn()");
    // df1.withColumn("newCol", df1.col("id") + df1.col("age")); // Attempt at cleaner syntax
    // df1 = df1.withColumn("newCol", (row) => row["id"] + row["age"]);
    // df1 = df1.withColumn("newCol", () => 1 + 2);
    // df1 = df1.withColumn("newCol", () => new Date());
    // df1 = df1.withColumn("newCol", (row) => row["somerandomprop"]); // Results in column with undefined values
    // df1 = df1.withColumn("1newCol", (row) => row["id"] + row["age"]); // Error - invalid column name
    // df1.head();
    df1 = df1.withColumn("newCol", () => 1);

    console.log("df1.groupBy()");
    console.log(df1.groupBy(["id", "age"]));


    console.log("df1.orderBy()");
    // df1.orderBy(["age"]).head();
    // df1.orderBy(["id", "age"], ["asc", "desc"]).head();
    // df1.orderBy("id").head(); // Error - need an array of columns


    console.log("df1 for...of iteration");
    for (let row of df1) {
        console.log(row);
    }

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

    await loris.writeTsv(df1, "df1_output.tsv");
    await loris.writeCsv(df1, "df1_output.csv");
    // await loris.writeDsv(df1, "df1_output.psv"); // Error
    // await loris.writeDsv(df1, "df1_output.psv", ""); // Error
    // await loris.writeDsv(df1, "df1_output.psv", {}); // Error
    await loris.writeDsv(df1, "df1_output.psv", "|");
    await loris.writeJson(df1, "df1_output.json");
}

test();


// console.log(new loris.DataFrame());
