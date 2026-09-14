# Lorix

[![CI](https://github.com/jmsmistral/lorix/actions/workflows/lorix-pr-test.yml/badge.svg?branch=master)](https://github.com/jmsmistral/lorix/actions/workflows/lorix-pr-test.yml)

<img align="right" src="docs/images/lorix.png" height="110" alt="Lorix logo">

Lorix is a _simple_, _user-friendly_ Javascript DataFrame API for loading and transforming data.

### Features

- Enables rapid data wrangling on Javascript
- Load and export data to/from text files and object arrays
- Exposes a simple, functional data-oriented API that operates over an array of objects
- Function chaining to encapsulate multiple data transformations in single blocks
- Helpful error messages to assist debugging

# Why Lorix?

I built this because I wanted a _simple_ way to wrangle data with Javascript for some web projects.
Rather than building something low-level from scratch, optimizing for performance, I opted to design a DataFrame abstraction
over existing libraries like _lodash_ and _d3_. The idea isn't for this to compare performance-wise with other libraries
(far from it!), but to provide an _intuitive_ API for anyone to pick-up and transform small to medium-sized datasets directly
in Javascript.

# How to install

Supported Node.js versions: `^22.13.0 || ^24.0.0`
- 22.x starting at 22.13.0; and
- 24.x starting at 24.0.0

Lorix uses ES modules. Save examples as `.mjs` files or set `"type": "module"` in your application's `package.json`.

```
npm install lorix
```

# Get Started

The examples below show individual operations. `df1` and `df2` represent DataFrames containing the columns used in each example; file-loading examples assume the named files exist.


### Create a DataFrame

```javascript
import lorix from "lorix";

let df1 = await lorix.readCsv("test.csv"); // Comma-separated file
let df2 = await lorix.readTsv("test.tsv"); // Tab-separated file
let df3 = await lorix.readDsv("test.psv", "|"); // User-specified delimiter

// Array of objects
// Note: All objects in the array need
// to have the same properties.
const dataArray = [
    {"colA": 1, "colB": 2},
    {"colA": 2, "colB": 3},
    {"colA": 3, "colB": 4}
];
let df4 = lorix.DataFrame.fromArray(dataArray);
```

### Print top _`n`_ rows

`.head(n)` prints the top _`n`_ rows in tabular form to standard output.

```javascript
df1.head(); // Print the top 10 rows by default
df1.head(15); // Define the number of rows to display
```

### Iterate DataFrame rows like an object array

The DataFrame class implements the iterator pattern to allow users to iterate through rows like an array. This also enables the use of the spread operator for example.

```javascript
for (let row of df1) {
    console.log(row);
}

let df = [...df1];
```

### Export DataFrame rows as an object array

`toArray()` returns an object array, where each object is a row mapping columns to values.

```javascript
let rowArray = df1.toArray();
```

### Select columns

`select(col1, col2, ...)` returns a new DataFrame with the specified columns.

```javascript
let df = df1.select("colA", "colB");
```

### Drop columns

`drop(col1, col2, ...)` returns a new DataFrame without the specified columns.

```javascript
let df = df1.drop("colA");
```

### Define new column

`withColumn(newCol, fn)` returns a new DataFrame with a new column (`newCol`), as defined by the function `fn`. `fn` accepts a single parameter that represents a DataFrame row to expose access to individual column values.

```javascript
let df = df1.withColumn("newCol", (row) => row["colA"] + row["colB"]);
```

Passing the row argument is not necessary if the expression doesn't use column values.

```javascript
let constantDf = df1.withColumn("newCol", () => 1 + 2);
let dateDf = df1.withColumn("newCol", () => new Date());
```

Calls can be chained to define multiple columns in a single block.

```javascript
let df = (
    df1
    .withColumn("newCol", () => 1)
    .withColumn("newCol2", () => 2)
);
```

### Filter rows

`filter(fn)` returns a new Dataframe with rows filtered according to the function `fn`. `fn` accepts a single parameter that represents a DataFrame row to expose access to individual column values, and must return a boolean value to determine if the row is filtered or not.

```javascript
let df = df1.filter(row => row["colA"] > 10);
```

### Replace strings

String values can be replaced using one of the following DataFrame methods. Each accepts an array of columns over which the string-replacement is applied:

- `replace(cols, oldString, newString)` - Returns a new DataFrame with first instance of string `oldString` replaced by `newString`.
- `replaceAll(cols, oldString, newString)` - Returns a new DataFrame with all instances of string `oldString` replaced by `newString`.
- `regexReplace(cols, replaceRegex, newString)` - Returns a new DataFrame with regular expression `replaceRegex` replaced by `newString`.

```javascript
let replacedDf = df1.replace(["colA", "colB"], "oldSubstring", "newSubstring");
let replacedAllDf = df1.replaceAll(["colA"], "oldSubstring", "newSubstring");
let regexDf = df1.regexReplace(["colA", "colB"], /oldSubstring/ig, "newSubstring");
```

### Drop duplicate rows

`distinct([subset])` returns a new DataFrame with duplicate rows dropped according to the optional list of columns `subset`. If `subset` is not passed, then duplicates will be identified across all columns. Only the first row found is kept for duplicate instances.

```javascript
let distinctDf = df1.distinct();
let distinctSubsetDf = df1.distinct(["colA", "colB"]);
```

### Sorting

`orderBy(cols, [order])` returns a new DataFrame with rows sorted according to the array of columns specified (`cols`), and optionally an array (`order`) defining the order to sort these by. The order defaults to _ascending_ if not specified.

```javascript
let sortedDf = df1.orderBy(["colA"]);
let multiSortDf = df1.orderBy(["colA", "colB"], ["asc", "desc"]);
// df1.orderBy("id"); // Error - requires an array of columns
```

### Joining DataFrames

Two DataFrames can be joined in a number of ways. Lorix provides functions that mirror SQL join types, and adds other types that appear in Spark:

- Cross Join
- Inner Join
- Left Join
- Right Join
- Left Anti Join
- Right Anti Join
- Full Outer Join

The join condition can be defined in the following ways:

1. a single array of common column names.
2. two arrays of the same size, with position-based joining between them.
3. function defining the exact join condition between the two DataFrames.

When using a function, its parameters represent a row from the left and right DataFrames. Use `&&` and `||` for logical conditions.

Cross join:

```javascript
let df = df1.crossJoin(df2);
```

Join on shared column names:

```javascript
let innerDf = df1.innerJoin(df2, ["colA", "colB"]);
let outerDf = df1.fullOuterJoin(df2, ["colA", "colB"]);
```

Join on different column names (`df1` has `colA` and `colC`; `df2` has `colB` and `colD`):

```javascript
let df = df1.innerJoin(df2, ["colA", "colC"], ["colB", "colD"]);
```

Define the join condition with a function:

```javascript
let innerDf = df1.innerJoin(df2, (l, r) => l.colA === r.colB);
let multiKeyDf = df1.innerJoin(
    df2,
    (l, r) => l.colA === r.colB && l.colC === r.colD,
);
let leftDf = df1.leftJoin(
    df2,
    (l, r) => l.colA === r.colB || l.colC === r.colD,
);
let rightDf = df1.rightJoin(df2, (l, r) => l.colA > r.colB && l.colC < r.colD);
let leftAntiDf = df1.leftAntiJoin(
    df2,
    (l, r) => l.colA === r.colB || l.colC === r.colD,
);
let rightAntiDf = df1.rightAntiJoin(
    df2,
    (l, r) => l.colA > r.colB && l.colC < r.colD,
);
```

Joins reject overlapping non-key columns to avoid overwriting data. Rename or drop those columns first. Cross joins suffix shared names with `_x` and `_y`. For compatibility, anti joins also include columns from the other frame, filled with `null`.

### Aggregating with groupBy

`groupBy(cols, aggMap)` is an analogue of SQL's GROUP BY, and is used to perform aggregations.

- `cols` is an array of columns that will be grouped.
- `aggMap` is an object mapping columns to the aggregations you want performed on these. This can either be an array, or a string (e.g. sum, mean, count).

Available aggregate functions are currently:

- sum
- mean
- count
- min
- max

Output columns are named using the current name suffixed by the aggregation applied, e.g. **colC_sum**, **colC_mean**.

```javascript
let df = df1.groupBy(
    ["colA", "colB"],
    {
        "colC": ["sum", "mean", "count"],
        "colD": "sum",
        "colE": ["min", "max"]
    }
);
```

### Window functions

`.window(windowFunc, [partitionByCols], [orderByCols], [windowSize])` can be applied within `.withColumn` to apply window function `windowFunc` to the DataFrame. The window parameters follow, defined as:

- `partitionByCols` is an optional array of columns used to partition the DataFrame rows.
- `orderByCols` is an optional array consisting of two sub-arrays - one defining the set of columns to sort, and another the sort order (see `.orderBy` for more details).
- `windowSize` is an optional array with two values defining the range of rows over which the window function is applied for each group. The first value defines the number of preceding rows to include in the window, and the second value being the number of following rows. If no `windowSize` parameter is passed, the entire set of rows is exposed to the window function for each group.
    - Non-negative integer representing the number of rows
    - `unboundedPreceding` all previous rows, relative to the current row
    - `unboundedProceeding` all following rows, relative to the current row
    - `currentRow` represents the current row

Bounds include the current row and stop at partition boundaries. Lag and lead return `null` when the exact offset row is absent. Unordered windows preserve input row order.

Lorix currently exposes the following window functions:

- `sum(col)` - sum of values.
- `min(col)` - minimum value.
- `max(col)` - maximum value.
- `mean(col)` - mean value.
- `median(col)` - median value.
- `quantile(col, p)` - returns the p-quantile, where p is a number in the range [0, 1].
- `variance(col)` - returns an unbiased estimator of the population variance.
- `stddev(col)` - returns the standard deviation, defined as the square root of the bias-corrected variance.
- `lag(col, n)` - returns the value of the `n`-th row prior to the current row.
- `lead(col, n)` - returns the value of the `n`-th row after the current row.
- `rownumber()` - returns the sequential number of a row within the partition.

```javascript
let df = df1.withColumn(
    "colStddev",
    lorix.window(
        lorix.stddev("colX"),   // window function (takes a column name, and any other required/option parameters)
        ["colA"],              // columns defining how rows are partitioned
        [["colB"], ["desc"]],  // optional - order columns
        [14, lorix.currentRow] // optional - window size definition (14 rows preceding to current row)
    )
);
```

`stdev` remains an alias for `stddev`, and the old `unboundedProceding` spelling remains supported.

### Union between DataFrames

`unionByName(df)` returns a new DataFrame including the set of rows from both DataFrames being unioned. Both DataFrames must have the same columns, otherwise an error will be thrown.

```javascript
let df = df1.unionByName(df2);
```

### Pivot values into columns

`pivot(groupColumns, pivotColumn, valueColumn, aggregation)` creates one column per pivot value. Output names use the value and aggregation, such as `red_sum`.

```javascript
let df = df1.pivot(["category"], "colour", "amount", "sum");
```

### Export to files

Writers return promises and accept relative or absolute paths. Existing files are replaced; the parent directory must exist.

```javascript
await lorix.writeCsv(df1, "output.csv");
await lorix.writeTsv(df1, "output.tsv");
await lorix.writeDsv(df1, "output.psv", "|");
await lorix.writeJson(df1, "output.json");
```

# Data behavior

- Transformations return new DataFrames, but row ownership is shallow. `toArray()` exposes references; treat rows and callback inputs as read-only.
- `distinct()` preserves value types and tuple boundaries. Objects, including Dates, compare by reference identity.
- File readers infer numbers, booleans and dates; empty cells become `null`. Use `fromArray()` with explicitly typed values when inference is inappropriate.
- `count` includes rows with null value cells. Pivot ignores null/undefined categories; missing categories produce zero for sum/count and null for other aggregates.
- Create an empty frame with `new lorix.DataFrame([], ["colA", "colB"])`. Empty results retain their column schema.

# Development

Run `npm ci` followed by `npm run check`. Example fixtures and expected results live in the tests, keeping the README focused on usage. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

# Changelog

See [CHANGELOG.md](CHANGELOG.md) for release history and compatibility notes.


# License

Free Software through the [GNU Affero GPL v3](https://www.gnu.org/licenses/why-affero-gpl.en.html)

See LICENSE file for details.
