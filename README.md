# Lorix

[![Actions Status](https://github.com/jmsmistral/lorix/workflows/Lorix%20Publish%20Workflow/badge.svg)](https://github.com/jmsmistral/lorix/actions)

<img align="right" src=docs/images/lorix.png height="110px">

Lorix is a _simple_, _user-friendly_ Javascript DataFrame API for loading and transforming data.

### Features

- Enables rapid data wrangling on Javascript
- Load and export data to/from text files and object arrays
- Exposes a simple, functional data-oriented API that operates over an array of objects
- Function chaining to encapsulate multiple data transformations in single blocks
- Helpful error messages to assist debugging

# Why Lorix?

I want a _simple_ way to wrangle data with Javascript for my own projects, instead of having to resort to pandas on Python.
Rather than building something low-level from scratch, optimizing for performance, I opted to design a DataFrame abstraction
over existing libraries like _lodash_ and _d3_. The idea isn't for this to compete performance-wise with other libraries
(far from it!), but to provide an _intuitive_ API for anyone to pick-up and transform small to medium-sized datasets directly
in Javascript.

# How to install

```
npm install lorix
```

# Get Started

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
    {"colA": 2, "colB": 3}
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
let df = df1.withColumn("newCol", () => 1 + 2);
let df = df1.withColumn("newCol", () => new Date());
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

### Drop duplicate rows

`distinct([subset])` returns a new DataFrame with duplicate rows dropped according to the optional list of columns `subset`. If `subset` is not passed, then duplicates will be identified across all columns. Only the first row found is kept for duplicate instances.

```javascript
let df = df1.distinct();
let df = df1.distinct(["colA", "colB"]);
```

### Sorting

`orderBy(cols, [order])` returns a new DataFrame with rows sorted according to the array of columns specified (`cols`), and optionally an array (`order`) defining the order to sort these by. The order defaults to _ascending_ if not specified.

```javascript
let df = df1.orderBy(["colA"]);
let df = df1.orderBy(["colA", "colB"], ["asc", "desc"]);
let df = df1.orderBy("id"); // Error - requires an array of columns
```

### Joining DataFrames

Two DataFrames can be joined in a number of ways. Lorix provides functions that mirror SQL join types, and adds other types that appear in Spark:
- Cross Join
- Inner Join
- Left Join
- Right Join
- Left Anti Join
- Right Anti Join
- Full Outer Join _(in development)_

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

let df = df1.leftAntiJoin(df2, (l, r) => (l.colA == r.colB) | (l.colC == r.colD));

let df = df1.rightAntiJoin(df2, (l, r) => (l.colA > r.colB) & (l.colC < r.colD));
```

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
- `windowSize` is an optional array with two values defining the range of rows over which the window function is applied for each group. The first value defines the number of preceding rows to include in the window, and the second value being the number of proceding rows. If no `windowSize` parameter is passed, the entire set of rows is exposed to the window function for each group.
    - Positive integer representing the number of rows
    - `unboundedPreceding` all previous rows, relative to the current row
    - `unboundedProceeding` all following rows, relative to the current row
    - `currentRow` represents the current row

Lorix currently exposes the following window functions:

- `sum(col)` - sum of values.
- `min(col)` - minimum value.
- `max(col)` - maximum value.
- `mean(col)` - mean value.
- `median(col)` - median value.
- `quantile(col, p)` - returns the p-quantile, where p is a number in the range [0, 1].
- `variance(col)` - returns an unbiased estimator of the population variance.
- `stdev(col)` - returns the standard deviation, defined as the square root of the bias-corrected variance.
- `lag(col, n)` - returns the value of the `n`-th row prior to the current row.
- `lead(col, n)` - returns the value of the `n`-th row after the current row.
- `rownumber()` - returns the sequential number of a row within the partition.

```javascript
let df = df1.withColumn(
    "colStddev",
    lorix.window(
        lorix.stdev("colX"),   // window function (takes a column name, and any other required/option parameters)
        ["colA"],              // columns defining how rows are partitioned
        [["colB"], ["desc"]],  // optional - order columns
        [14, lorix.currentRow] // optional - window size definition (14 rows preceding to current row)
    )
);
```

# License

Free Software through the [GNU Affero GPL v3](https://www.gnu.org/licenses/why-affero-gpl.en.html)

See LICENSE file for details.
