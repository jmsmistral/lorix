# Loris

<img align="right" src=docs/images/loris.png height="150px">

Loris is a _simple_, _user-friendly_ Javascript Dataframe API for loading and transforming data.

### Features

- Enables rapid data analysis and exploration on Javascript
- Load and export data to/from text files
- Exposes a functional data-oriented API that operates over an array of objects
- Easily convert Dataframe to row of objects


# Get Started

## Create a Dataframe

```javascript
const df1 = await loris.readCsv("test.csv"); // Comma-separated
const df2 = await loris.readTsv("test.tsv"); // Tab-separated
const df3 = await loris.readDsv("test.psv", "|"); // User-specified delimiter
```

## Print top n rows

```javascript
df1.head(); // Print the top 10 rows by default
df1.head(15); // Define the number of rows to display
```

## Select columns

```javascript
let df = df1.select("colA", "colB");
```

## Drop columns

```javascript
let df = df1.drop("colA");
```

## Define new columns

```javascript
// Pass a function that returns an expression
// row represents a row object from the Dataframe, where column values can be accessed as below
let df = df1.withColumn("newCol", (row) => row["colA"] + row["colB"]);

// Passing the row argument is not necessary if the expression doesn't use column values
let df = df1.withColumn("newCol", () => 1 + 2);
let df = df1.withColumn("newCol", () => new Date());

// Calls return a new Dataframe, so can be chained to define multiple columns in one block
let df = (
    df1
    .withColumn("newCol", () => 1)
    .withColumn("newCol2", () => 2)
);
```


## Aggregating with groupBy()

```javascript
// Apply aggregations to a Dataframe
// The first parameter is an array of column names that will be grouped
// The second parameter is an object, where keys are columns to be aggregated,
// and the values are either lists of aggregations, or a string defining a single
// aggregation (e.g. sum, mean, count)
// Output columns are named using the current name suffixed by the aggregation
// applied, e.g. newCol_sum, newCol_mean.
let df = df1.groupBy(
    ["colA", "colB"],
    {
        "newCol": ["sum", "mean", "count"],
        "newCol2": "sum"
    }
);
```

## Aggregating with window()

```javascript
<being created>
```


## Join Dataframes

```javascript
let df = df1.crossJoin(df2)

// Joins can be defined in the following ways:
// 1. a single array of common column names.
// 2. two arrays of the same size, with position-based joining between them.
// 3. function defining the exact join condition between the two Dataframes.
// When using a function, the parameters represent left and right Dataframes
// being joined. These are used to refer to the Dataframe columns.
let df = df1.innerJoin(df2, ["colA", "colB"]);  // Columns must exist in both Dataframes
let df = df1.innerJoin(df2, ["colA", "colC"], ["colB", "colD"]);  // Join based on colA == colB and colC == colD
let df = df1.innerJoin(df2, (l, r) => l.colA == r.colB);
let df = df1.innerJoin(df2, (l, r) => (l.colA == r.colB) & (l.colC == r.colD));

df1.leftJoin(df2, (l, r) => (l.colA == r.colB) | (l.colC == r.colD));

df1.rightJoin(df2, (l, r) => (l.colA > r.colB) & (l.colC < r.colD));

```
