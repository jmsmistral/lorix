import { DataFrame } from "../src/dataframe.js";
import { readCsv } from "../src/io.js";


// Iris dataset
export let iris = await readCsv("./test/data/iris.csv");

// Very Small DataFrames
export let verySmallDataFrame1 = (
    new DataFrame(
        [
            {"id": 1, "name": "billy"},
            {"id": 2, "name": "jane"},
            {"id": 3, "name": "roger"}
        ],
        ["id", "name"]
    )
);

export let verySmallDataFrame2 = (
    new DataFrame(
        [
            {"id": 1, "name": "billy", "age": 10},
            {"id": 2, "name": "jane", "age": 20},
            {"id": 4, "name": "gary", "age": 40}
        ],
        ["id", "name", "age"]
    )
);

export let verySmallDataFrame3 = (
    new DataFrame(
        [
            {"idCol": 1, "name": "billy", "age": 10},
            {"idCol": 2, "name": "jane", "age": 20},
            {"idCol": 4, "name": "gary", "age": 40}
        ],
        ["idCol", "name", "age"]
    )
);

export let verySmallDataFrame4 = (
    new DataFrame(
        [
            {"gender": "male", "id": 1, "name": "billy"},
            {"gender": "male", "id": 2, "name": "jane"},
            {"gender": "female", "id": 3, "name": "roger"}
        ],
        ["gender", "id", "name"]
    )
);

export let verySmallValidObjArray = [
    {"id": 1, "name": "billy"},
    {"id": 2, "name": "jane"},
    {"id": 3, "name": "roger"}
];

export let verySmallInvalidObjArray = [
    {"id": 1, "name": "billy"},
    {"name": "jane"},
    {"id": 3, "name": "roger"}
];


// Small DataFrames
export let smallDataFrame1 = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 54, "name": "wayne", "weight": 87 },
            { "id": 78, "name": "carl", "weight": 86 },
            { "id": 23, "name": "fred", "weight": 62 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 201, "name": "alex", "weight": 95 },
            { "id": 169, "name": "dwayne", "weight": 99 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 45, "name": "issac", "weight": 104 }
          ],
        ["id", "name", "weight"]
    )
);

export let smallDataFrame2 = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 1000, "name": "billy", "weight": 102 },
            { "id": 1001, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 }
          ],
        ["id", "name", "weight"]
    )
);

export let smallDataFrame3 = (
    new DataFrame(
        [
            {"name": "Beverly", "dept": 2, "salary": 16231},
            {"name": "Kameko",  "dept": 2, "salary": 16232},
            {"name": "Ursa",    "dept": 3, "salary": 15427},
            {"name": "Ferris",  "dept": 1, "salary": 19740},
            {"name": "Noel",    "dept": 1, "salary": 19745},
            {"name": "Abel",    "dept": 3, "salary": 12369},
            {"name": "Raphael", "dept": 1, "salary": 8227},
            {"name": "Jack",    "dept": 1, "salary": 9710},
            {"name": "May",     "dept": 3, "salary": 9308},
            {"name": "Haviva",  "dept": 2, "salary": 9308}
        ],
        ["name", "dept", "salary"]
    )
);

export let smallDataFrame4 = (
    new DataFrame(
        [
            { "id": 100,  "name": "billy",    "colour": "red",    "weight": 102 },
            { "id": 2,    "name": "jane",     "colour": "blue",   "weight": 97 },
            { "id": 5,    "name": "roger",    "colour": "green",  "weight": 107 },
            { "id": 9,    "name": "gary",     "colour": "blue",   "weight": 87 },
            { "id": 1,    "name": "joseph",   "colour": "yellow", "weight": 71 },
            { "id": 3,    "name": "jennifer", "colour": "yellow", "weight": 84 },
            { "id": 100,  "name": "sean",     "colour": "red",    "weight": 85 },
            { "id": 17,   "name": "steven",   "colour": "orange", "weight": 107 },
            { "id": 101,  "name": "elon",     "colour": "red",    "weight": 74 },
            { "id": 45,   "name": "issac",    "colour": "green",  "weight": 104 },
            { "id": 1000, "name": "billy",    "colour": "orange", "weight": 102 },
            { "id": 1001, "name": "billy",    "colour": "purple", "weight": 102 },
            { "id": 2,    "name": "jane",     "colour": "black",  "weight": 97 }
          ],
        ["id", "name", "colour", "weight"]
    )
);

export let smallDataFrame5 = (
    new DataFrame(
        [
            { "id": 100,  "name": "billy",    "colour": "red",    "weight": 102 },
            { "id": 2,    "name": "jane",     "colour": "blue",   "weight": 97 },
            { "id": 2,    "name": "jane",     "colour": "red",   "weight": 97 },
            { "id": 2,    "name": "jane",     "colour": "red",   "weight": 97 },
            { "id": 2,    "name": "jane",     "colour": null,   "weight": 97 },
            { "id": 5,    "name": "roger",    "colour": "green",  "weight": 107 },
            { "id": 9,    "name": "gary",     "colour": "blue",   "weight": 87 },
            { "id": 1,    "name": "joseph",   "colour": "yellow", "weight": 71 },
            { "id": 3,    "name": "jennifer", "colour": "yellow", "weight": 84 },
            { "id": 3,    "name": "jennifer", "colour": "orange", "weight": 84 },
            { "id": 100,  "name": "sean",     "colour": "red",    "weight": 85 },
            { "id": 17,   "name": "steven",   "colour": "orange", "weight": 107 },
            { "id": 17,   "name": "steven",   "colour": null, "weight": 107 },
            { "id": 17,   "name": "steven",   "colour": null, "weight": 107 },
            { "id": 101,  "name": "elon",     "colour": "red",    "weight": 74 },
            { "id": 45,   "name": "issac",    "colour": "green",  "weight": 104 },
            { "id": 45,   "name": "issac",    "colour": "green",  "weight": 104 },
            { "id": 45,   "name": "issac",    "colour": "green",  "weight": 104 },
            { "id": 1000, "name": "billy",    "colour": "orange", "weight": 102 },
            { "id": 1001, "name": "billy",    "colour": "purple", "weight": 102 },
            { "id": 1001, "name": "billy",    "colour": "purple", "weight": 102 },
            { "id": 2,    "name": "jane",     "colour": "black",  "weight": 97 },
          ],
        ["id", "name", "colour", "weight"]
    )
);


// Test result validation datasets

// Cross join of verySmallDataFrame1 with itself
export let verySmallDataFrameCrossJoinResult = (
    new DataFrame(
        [
            { "id_x": 1, "name_x": "billy", "id_y": 1, "name_y": "billy" },
            { "id_x": 1, "name_x": "billy", "id_y": 2, "name_y": "jane" },
            { "id_x": 1, "name_x": "billy", "id_y": 3, "name_y": "roger" },
            { "id_x": 2, "name_x": "jane", "id_y": 1, "name_y": "billy" },
            { "id_x": 2, "name_x": "jane", "id_y": 2, "name_y": "jane" },
            { "id_x": 2, "name_x": "jane", "id_y": 3, "name_y": "roger" },
            { "id_x": 3, "name_x": "roger", "id_y": 1, "name_y": "billy" },
            { "id_x": 3, "name_x": "roger", "id_y": 2, "name_y": "jane" },
            { "id_x": 3, "name_x": "roger", "id_y": 3, "name_y": "roger" }
        ],
        ["id_x", "name_x", "id_y", "name_y"]
    )
);

// Inner join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameInnerJoinResult = (
    new DataFrame(
        [
            { "id": 1, "name": "billy", "age": 10 },
            { "id": 2, "name": "jane", "age": 20 }
        ],
        ["id", "name", "age"]
    )
);

// Left join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameLeftJoinResult = (
    new DataFrame(
        [
            { "id": 1, "name": "billy", "age": 10 },
            { "id": 2, "name": "jane", "age": 20 },
            { "id": 3, "name": "roger", "age": null }
        ],
        ["id", "name", "age"]
    )
);

// Right join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameRightJoinResult = (
    new DataFrame(
        [
            { "id": 1, "name": "billy", "age": 10 },
            { "id": 2, "name": "jane", "age": 20 },
            { "id": 4, "name": "gary", "age": 40 }
        ],
        ["id", "name", "age"]
    )
);

// Left anti join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameLeftAntiJoinResult = (
    new DataFrame(
        [
            { "id": 3, "name": "roger", "age": null }
        ],
        ["id", "name", "age"]
    )
);

// Right anti join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameRightAntiJoinResult = (
    new DataFrame(
        [
            { "id": 4, "name": "gary", "age": 40 }
        ],
        ["id", "name", "age"]
    )
);

// Full outer join of verySmallDataFrame2 and verySmallDataFrame4
export let verySmallDataFrameFullOuterJoinResult = (
    new DataFrame(
        [
            { "id": 1, "name": "billy", "age": 10, "gender": "male" },
            { "id": 2, "name": "jane", "age": 20, "gender": "male" },
            { "id": 4, "name": "gary", "age": 40, "gender": null },
            { "age": null, "gender": "female", "id": 3, "name": "roger" }
        ],
        ["id", "name", "age", "gender"]
    )
);

// orderBy of smallDataFrame1 by id
export let smallDataFrame1OrderByIdResult = (
    new DataFrame(
        [
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 23, "name": "fred", "weight": 62 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 54, "name": "wayne", "weight": 87 },
            { "id": 78, "name": "carl", "weight": 86 },
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 169, "name": "dwayne", "weight": 99 },
            { "id": 201, "name": "alex", "weight": 95 }
        ],
        ["id", "name", "weight"]
    )
);

// orderBy of smallDataFrame1 by name
export let smallDataFrame1OrderByNameResult = (
    new DataFrame(
        [
            { "id": 201, "name": "alex", "weight": 95 },
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 78, "name": "carl", "weight": 86 },
            { "id": 169, "name": "dwayne", "weight": 99 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 23, "name": "fred", "weight": 62 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 54, "name": "wayne", "weight": 87 }
        ],
        ["id", "name", "weight"]
    )
);

// orderBy of smallDataFrame1 by id, weight
export let smallDataFrame1OrderByIdWeightResult = (
    new DataFrame(
        [
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 23, "name": "fred", "weight": 62 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 54, "name": "wayne", "weight": 87 },
            { "id": 78, "name": "carl", "weight": 86 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 169, "name": "dwayne", "weight": 99 },
            { "id": 201, "name": "alex", "weight": 95 }
        ],
        ["id", "name", "weight"]
    )
);

// orderBy of smallDataFrame1 by id (desc), weight (asc)
export let smallDataFrame1OrderByIdDescWeightAscResult = (
    new DataFrame(
        [
            { "id": 201, "name": "alex", "weight": 95 },
            { "id": 169, "name": "dwayne", "weight": 99 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 78, "name": "carl", "weight": 86 },
            { "id": 54, "name": "wayne", "weight": 87 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 23, "name": "fred", "weight": 62 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 1, "name": "joseph", "weight": 71 }
        ],
        ["id", "name", "weight"]
    )
);

// groupBy of iris by
export let irisGroupBySpeciesResult = (
    new DataFrame(
        [
            {
              species: "setosa",
              sepal_length_min: 4.3,
              sepal_length_max: 5.8,
              sepal_length_mean: 5.005999999999999,
              sepal_length_count: 50,
              sepal_length_sum: 250.29999999999998
            },
            {
              species: "versicolor",
              sepal_length_min: 4.9,
              sepal_length_max: 7,
              sepal_length_mean: 5.936,
              sepal_length_count: 50,
              sepal_length_sum: 296.8
            },
            {
              species: "virginica",
              sepal_length_min: 4.9,
              sepal_length_max: 7.9,
              sepal_length_mean: 6.587999999999998,
              sepal_length_count: 50,
              sepal_length_sum: 329.3999999999999
            }
        ],
        [
            "species",
            "sepal_length_min",
            "sepal_length_max",
            "sepal_length_mean",
            "sepal_length_count",
            "sepal_length_sum"
        ]
    )
);


//  filter results
export let smallDataFrame1FilterIdResult = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 100, "name": "sean", "weight": 85 }
        ],
        ["id", "name", "weight"]
    )
);

export let smallDataFrame1FilterWeightResult = (
    new DataFrame(
        [
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 23, "name": "fred", "weight": 62 },
            { "id": 101, "name": "elon", "weight": 74 }
        ],
        ["id", "name", "weight"]
    )
);



// distinct results
export let smallDataFrame2DistinctAllResult = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 1000, "name": "billy", "weight": 102 },
            { "id": 1001, "name": "billy", "weight": 102 }
        ],
        ["id", "name", "weight"]
    )
);

export let smallDataFrame2DistinctIdResult = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 1000, "name": "billy", "weight": 102 },
            { "id": 1001, "name": "billy", "weight": 102 }
        ],
        ["id", "name", "weight"]
    )
);

export let smallDataFrame2DistinctNameWeightResult = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 45, "name": "issac", "weight": 104 }
        ],
        ["id", "name", "weight"]
    )
);


// window functions
export let smallDataFrame3WindowFuncsOrderedPartition = (
    new DataFrame(
        [
            { "name": "Noel",    "dept": 1, "salary": 19745, "sum": 57422, "lag": null,  "lead": 19740, "stddev": 6250, "rownum": 1 },
            { "name": "Ferris",  "dept": 1, "salary": 19740, "sum": 57422, "lag": 19745, "lead": 9710,  "stddev": 6250, "rownum": 2 },
            { "name": "Jack",    "dept": 1, "salary": 9710,  "sum": 57422, "lag": 19740, "lead": 8227,  "stddev": 6250, "rownum": 3 },
            { "name": "Raphael", "dept": 1, "salary": 8227,  "sum": 57422, "lag": 9710,  "lead": null,  "stddev": 6250, "rownum": 4 },
            { "name": "Kameko",  "dept": 2, "salary": 16232, "sum": 41771, "lag": null,  "lead": 16231, "stddev": 3997, "rownum": 1 },
            { "name": "Beverly", "dept": 2, "salary": 16231, "sum": 41771, "lag": 16232, "lead": 9308,  "stddev": 3997, "rownum": 2 },
            { "name": "Haviva",  "dept": 2, "salary": 9308,  "sum": 41771, "lag": 16231, "lead": null,  "stddev": 3997, "rownum": 3 },
            { "name": "Ursa",    "dept": 3, "salary": 15427, "sum": 37104, "lag": null,  "lead": 12369, "stddev": 3060, "rownum": 1 },
            { "name": "Abel",    "dept": 3, "salary": 12369, "sum": 37104, "lag": 15427, "lead": 9308,  "stddev": 3060, "rownum": 2 },
            { "name": "May",     "dept": 3, "salary": 9308,  "sum": 37104, "lag": 12369, "lead": null,  "stddev": 3060, "rownum": 3 }
        ],
        ["name", "dept", "salary", "sum", "lag", "lead", "stddev", "rownum"]
    )
);


export let smallDataFrame3WindowFuncsWindowSize = (
    new DataFrame(
        [
            {"name" : "Noel",    "dept" : 1, "salary" : 19745, "sum" : 19745, "lag" : null,  "lead" : 19740, "stddev" : null, "variance" : null,     "mean" : 19745, "median" : 19745, "min" : 19745, "max" : 19745, "rownum" : 1},
            {"name" : "Ferris",  "dept" : 1, "salary" : 19740, "sum" : 39485, "lag" : 19745, "lead" : 9710,  "stddev" : 4,    "variance" : 13,       "mean" : 19743, "median" : 19743, "min" : 19740, "max" : 19745, "rownum" : 2},
            {"name" : "Jack",    "dept" : 1, "salary" : 9710,  "sum" : 29450, "lag" : 19740, "lead" : 8227,  "stddev" : 7092, "variance" : 50300450, "mean" : 14725, "median" : 14725, "min" : 9710,  "max" : 19740, "rownum" : 3},
            {"name" : "Raphael", "dept" : 1, "salary" : 8227,  "sum" : 17937, "lag" : 9710,  "lead" : null,  "stddev" : 1049, "variance" : 1099645,  "mean" : 8969,  "median" : 8969,  "min" : 8227,  "max" : 9710,  "rownum" : 4},
            {"name" : "Kameko",  "dept" : 2, "salary" : 16232, "sum" : 16232, "lag" : null,  "lead" : 16231, "stddev" : null, "variance" : null,     "mean" : 16232, "median" : 16232, "min" : 16232, "max" : 16232, "rownum" : 1},
            {"name" : "Beverly", "dept" : 2, "salary" : 16231, "sum" : 32463, "lag" : 16232, "lead" : 9308,  "stddev" : 1,    "variance" : 1,        "mean" : 16232, "median" : 16232, "min" : 16231, "max" : 16232, "rownum" : 2},
            {"name" : "Haviva",  "dept" : 2, "salary" : 9308,  "sum" : 25539, "lag" : 16231, "lead" : null,  "stddev" : 4895, "variance" : 23963965, "mean" : 12770, "median" : 12770, "min" : 9308,  "max" : 16231, "rownum" : 3},
            {"name" : "Ursa",    "dept" : 3, "salary" : 15427, "sum" : 15427, "lag" : null,  "lead" : 12369, "stddev" : null, "variance" : null,     "mean" : 15427, "median" : 15427, "min" : 15427, "max" : 15427, "rownum" : 1},
            {"name" : "Abel",    "dept" : 3, "salary" : 12369, "sum" : 27796, "lag" : 15427, "lead" : 9308,  "stddev" : 2162, "variance" : 4675682,  "mean" : 13898, "median" : 13898, "min" : 12369, "max" : 15427, "rownum" : 2},
            {"name" : "May",     "dept" : 3, "salary" : 9308,  "sum" : 21677, "lag" : 12369, "lead" : null,  "stddev" : 2164, "variance" : 4684861,  "mean" : 10839, "median" : 10839, "min" : 9308,  "max" : 12369, "rownum" : 3}
        ],
        ["name", "dept", "salary", "sum", "lag", "lead", "stddev", "variance", "mean", "median", "min", "max", "rownum"]
    )
);

// replace Name r -> rrr
export let smallDataFrame4ReplaceName = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "colour": "red", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "blue", "weight": 97 },
            { "id": 5, "name": "rrroger", "colour": "green", "weight": 107 },
            { "id": 9, "name": "garrry", "colour": "blue", "weight": 87 },
            { "id": 1, "name": "joseph", "colour": "yellow", "weight": 71 },
            { "id": 3, "name": "jenniferrr", "colour": "yellow", "weight": 84 },
            { "id": 100, "name": "sean", "colour": "red", "weight": 85 },
            { "id": 17, "name": "steven", "colour": "orange", "weight": 107 },
            { "id": 101, "name": "elon", "colour": "red", "weight": 74 },
            { "id": 45, "name": "issac", "colour": "green", "weight": 104 },
            { "id": 1000, "name": "billy", "colour": "orange", "weight": 102 },
            { "id": 1001, "name": "billy", "colour": "purple", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "black", "weight": 97 }
        ],
        ["id", "name", "colour", "weight"]
    )
);

// replace Name, Colour r -> rrr
export let smallDataFrame4ReplaceNameColour = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "colour": "rrred", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "blue", "weight": 97 },
            { "id": 5, "name": "rrroger", "colour": "grrreen", "weight": 107 },
            { "id": 9, "name": "garrry", "colour": "blue", "weight": 87 },
            { "id": 1, "name": "joseph", "colour": "yellow", "weight": 71 },
            { "id": 3, "name": "jenniferrr", "colour": "yellow", "weight": 84 },
            { "id": 100, "name": "sean", "colour": "rrred", "weight": 85 },
            { "id": 17, "name": "steven", "colour": "orrrange", "weight": 107 },
            { "id": 101, "name": "elon", "colour": "rrred", "weight": 74 },
            { "id": 45, "name": "issac", "colour": "grrreen", "weight": 104 },
            { "id": 1000, "name": "billy", "colour": "orrrange", "weight": 102 },
            { "id": 1001, "name": "billy", "colour": "purrrple", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "black", "weight": 97 }
        ],
        ["id", "name", "colour", "weight"]
    )
);

// replaceAll Name r -> rrr
export let smallDataFrame4ReplaceAllName = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "colour": "red", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "blue", "weight": 97 },
            { "id": 5, "name": "rrrogerrr", "colour": "green", "weight": 107 },
            { "id": 9, "name": "garrry", "colour": "blue", "weight": 87 },
            { "id": 1, "name": "joseph", "colour": "yellow", "weight": 71 },
            { "id": 3, "name": "jenniferrr", "colour": "yellow", "weight": 84 },
            { "id": 100, "name": "sean", "colour": "red", "weight": 85 },
            { "id": 17, "name": "steven", "colour": "orange", "weight": 107 },
            { "id": 101, "name": "elon", "colour": "red", "weight": 74 },
            { "id": 45, "name": "issac", "colour": "green", "weight": 104 },
            { "id": 1000, "name": "billy", "colour": "orange", "weight": 102 },
            { "id": 1001, "name": "billy", "colour": "purple", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "black", "weight": 97 }
        ],
        ["id", "name", "colour", "weight"]
    )
);

// replaceAll Name, Colour r -> rrr
export let smallDataFrame4ReplaceAllNameColour = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "colour": "rrred", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "blue", "weight": 97 },
            { "id": 5, "name": "rrrogerrr", "colour": "grrreen", "weight": 107 },
            { "id": 9, "name": "garrry", "colour": "blue", "weight": 87 },
            { "id": 1, "name": "joseph", "colour": "yellow", "weight": 71 },
            { "id": 3, "name": "jenniferrr", "colour": "yellow", "weight": 84 },
            { "id": 100, "name": "sean", "colour": "rrred", "weight": 85 },
            { "id": 17, "name": "steven", "colour": "orrrange", "weight": 107 },
            { "id": 101, "name": "elon", "colour": "rrred", "weight": 74 },
            { "id": 45, "name": "issac", "colour": "grrreen", "weight": 104 },
            { "id": 1000, "name": "billy", "colour": "orrrange", "weight": 102 },
            { "id": 1001, "name": "billy", "colour": "purrrple", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "black", "weight": 97 }
        ],
        ["id", "name", "colour", "weight"]
    )
);


// regexReplace Name r -> rrr
export let smallDataFrame4RegexReplaceName = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "colour": "red", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "blue", "weight": 97 },
            { "id": 5, "name": "rrroger", "colour": "green", "weight": 107 },
            { "id": 9, "name": "garrry", "colour": "blue", "weight": 87 },
            { "id": 1, "name": "joseph", "colour": "yellow", "weight": 71 },
            { "id": 3, "name": "jenniferrr", "colour": "yellow", "weight": 84 },
            { "id": 100, "name": "sean", "colour": "red", "weight": 85 },
            { "id": 17, "name": "steven", "colour": "orange", "weight": 107 },
            { "id": 101, "name": "elon", "colour": "red", "weight": 74 },
            { "id": 45, "name": "issac", "colour": "green", "weight": 104 },
            { "id": 1000, "name": "billy", "colour": "orange", "weight": 102 },
            { "id": 1001, "name": "billy", "colour": "purple", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "black", "weight": 97 }
        ],
        ["id", "name", "colour", "weight"]
    )
);

// regexReplace Name r -> rrr Global
export let smallDataFrame4RegexReplaceNameGlobal = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "colour": "red", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "blue", "weight": 97 },
            { "id": 5, "name": "rrrogerrr", "colour": "green", "weight": 107 },
            { "id": 9, "name": "garrry", "colour": "blue", "weight": 87 },
            { "id": 1, "name": "joseph", "colour": "yellow", "weight": 71 },
            { "id": 3, "name": "jenniferrr", "colour": "yellow", "weight": 84 },
            { "id": 100, "name": "sean", "colour": "red", "weight": 85 },
            { "id": 17, "name": "steven", "colour": "orange", "weight": 107 },
            { "id": 101, "name": "elon", "colour": "red", "weight": 74 },
            { "id": 45, "name": "issac", "colour": "green", "weight": 104 },
            { "id": 1000, "name": "billy", "colour": "orange", "weight": 102 },
            { "id": 1001, "name": "billy", "colour": "purple", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "black", "weight": 97 }
        ],
        ["id", "name", "colour", "weight"]
    )
);

// regexReplace Name, Colour r -> rrr Global
export let smallDataFrame4RegexReplaceNameColourGlobal = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "colour": "rrred", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "blue", "weight": 97 },
            { "id": 5, "name": "rrrogerrr", "colour": "grrreen", "weight": 107 },
            { "id": 9, "name": "garrry", "colour": "blue", "weight": 87 },
            { "id": 1, "name": "joseph", "colour": "yellow", "weight": 71 },
            { "id": 3, "name": "jenniferrr", "colour": "yellow", "weight": 84 },
            { "id": 100, "name": "sean", "colour": "rrred", "weight": 85 },
            { "id": 17, "name": "steven", "colour": "orrrange", "weight": 107 },
            { "id": 101, "name": "elon", "colour": "rrred", "weight": 74 },
            { "id": 45, "name": "issac", "colour": "grrreen", "weight": 104 },
            { "id": 1000, "name": "billy", "colour": "orrrange", "weight": 102 },
            { "id": 1001, "name": "billy", "colour": "purrrple", "weight": 102 },
            { "id": 2, "name": "jane", "colour": "black", "weight": 97 }
        ],
        ["id", "name", "colour", "weight"]
    )
);

// unionByName
export let smallDataFrame1and2UnionByName = (
    new DataFrame(
        [
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 54, "name": "wayne", "weight": 87 },
            { "id": 78, "name": "carl", "weight": 86 },
            { "id": 23, "name": "fred", "weight": 62 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 201, "name": "alex", "weight": 95 },
            { "id": 169, "name": "dwayne", "weight": 99 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 100, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 },
            { "id": 5, "name": "roger", "weight": 107 },
            { "id": 9, "name": "gary", "weight": 87 },
            { "id": 1, "name": "joseph", "weight": 71 },
            { "id": 3, "name": "jennifer", "weight": 84 },
            { "id": 100, "name": "sean", "weight": 85 },
            { "id": 17, "name": "steven", "weight": 107 },
            { "id": 101, "name": "elon", "weight": 74 },
            { "id": 45, "name": "issac", "weight": 104 },
            { "id": 1000, "name": "billy", "weight": 102 },
            { "id": 1001, "name": "billy", "weight": 102 },
            { "id": 2, "name": "jane", "weight": 97 }
        ],
        ["id", "name", "weight"]
    )
);
