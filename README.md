# Loris

<img align="right" src=docs/images/loris.png height="110px">

Loris is a _simple_, _user-friendly_ Javascript DataFrame API for loading and transforming data.

### Features

- Enables rapid data wrangling and exploration on Javascript
- Load and export data to/from text files
- Exposes a functional data-oriented API that operates over an array of objects
- Function chaining to create encapsulated data transformations

# Get Started

### Create a DataFrame

```javascript
let df1 = await loris.readCsv("test.csv"); // Comma-separated file
let df2 = await loris.readTsv("test.tsv"); // Tab-separated file
let df3 = await loris.readDsv("test.psv", "|"); // User-specified delimiter

// Array of objects
// Note: All objects in the array need
// to have the same properties.
const dataArray = [
    {"colA": 1, "colB": 2},
    {"colA": 2, "colB": 3}
    {"colA": 3, "colB": 4}
];
let df4 = loris.DataFrame.fromArray(dataArray);
```

### Print top **`n`** rows

```javascript
df1.head(); // Print the top 10 rows by default
df1.head(15); // Define the number of rows to display
```

### Iterate DataFrame rows like an object array

```javascript
for (let row of df1) {
    console.log(row);
}
```

### Select columns

```javascript
let df = df1.select("colA", "colB");
```

### Drop columns

```javascript
let df = df1.drop("colA");
```

### Define new column

Pass a function that returns an expression row represents a row object from the DataFrame, where column values can be accessed as below.
```javascript
let df = df1.withColumn("newCol", (row) => row["colA"] + row["colB"]);
```

Passing the row argument is not necessary if the expression doesn't use column values.
```javascript
let df = df1.withColumn("newCol", () => 1 + 2);
let df = df1.withColumn("newCol", () => new Date());
```

Calls return a new DataFrame, so can be chained to define multiple columns in one block
```javascript
let df = (
    df1
    .withColumn("newCol", () => 1)
    .withColumn("newCol2", () => 2)
);
```

### Sorting

The `.orderBy()` function of a DataFrame sorts rows according to the array of columns specified, and optionally the order to sort these by (defaults to ascending order).
```javascript
let df = df1.orderBy(["colA"]);
let df = df1.orderBy(["colA", "colB"], ["asc", "desc"]);
let df = df1.orderBy("id").head(); // Error - requires an array of columns
```

### Aggregating with groupBy

Use the `.groupBy()` function of a DataFrame as an analogue of SQL's GROUP BY to perform aggregations.
- The first parameter is an array of columns that will be grouped.
- The second parameter is an object mapping columns to the aggregations you want performed on these. This can either be an array, or a string (e.g. sum, mean, count).

Output columns are named using the current name suffixed by the aggregation applied, e.g. **colC_sum**, **colC_mean**.
```javascript
let df = df1.groupBy(
    ["colA", "colB"],
    {
        "colC": ["sum", "mean", "count"],
        "colD": "sum"
    }
);
```

### Aggregating with window functions

```
<in development>
```


### Joining DataFrames

Two DataFrames can be joined in a number of ways. Loris provides functions that mirror SQL join types, and adds other types that appear in Spark:
- Cross Join
- Inner Join
- Left Join
- Right Join
- Left Anti Join (_in development_)
- Right Anti Join (_in development_)

The join condition can be defined in the following ways:
1. a single array of common column names.
2. two arrays of the same size, with position-based joining between them.
3. function defining the exact join condition between the two DataFrames.

When using a function, the parameters represent left and right DataFrames
being joined. These are used to refer to the DataFrame columns.

```javascript
let df = df1.crossJoin(df2)

let df = df1.innerJoin(df2, ["colA", "colB"]);  // Columns must exist in both DataFrames
let df = df1.innerJoin(df2, ["colA", "colC"], ["colB", "colD"]);  // Equivalent to colA == colB and colC == colD
let df = df1.innerJoin(df2, (l, r) => l.colA == r.colB);
let df = df1.innerJoin(df2, (l, r) => (l.colA == r.colB) & (l.colC == r.colD));

let df = df1.leftJoin(df2, (l, r) => (l.colA == r.colB) | (l.colC == r.colD));

let df = df1.rightJoin(df2, (l, r) => (l.colA > r.colB) & (l.colC < r.colD));
```

### Get DataFrame rows as object array

```javascript
df1.toArray();
```


# License

Free Software through the [GNU Affero GPL v3](https://www.gnu.org/licenses/why-affero-gpl.en.html)

See LICENSE file for details.
