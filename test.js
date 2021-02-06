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


    // loading data data
    let df1 = await loris.readCsv('test.csv');
    let df2 = await loris.readCsv('test2.csv');

    for (let row of df1) {
        console.log(row);
    }

    // selecting columns
    // console.log(df);
    // const df1 = df.select('id', 'name')
    // console.log(df1);
    // const df2 = df1.select('id');
    // console.log(df2);
    // df.head();

    // dropping columns
    // let df3 = df.drop('name');

    df1.head();
    df2.head();

    // joins
    console.log('cross join');
    // (df1.crossJoin(df2, (l, r) => l.id == r.id)).head(); // Error - number of arguments
    // (df1.crossJoin(df2)).head(); // Cross join

    console.log('inner join');
    // (df1.innerJoin(df2, (l, r) => l.id == r.id, false)).head(); // Error - number of arguments
    // (df1.innerJoin(df2, "id")).head(); // Error - argument types
    // (df1.innerJoin(df2, (l, r) => l.id == r.id)).head();  // Non-indexed inner join
    (df1.innerJoin(df2, (l, r) => (l.id == r.id) & (l.age == r.age))).head(); // Non-indexed inner join
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
}

test();


// console.log(new loris.DataFrame());
