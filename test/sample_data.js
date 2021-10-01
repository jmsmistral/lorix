import { DataFrame } from '../src/dataframe.js';
import { readCsv } from '../src/io.js';


// Iris dataset
export let iris = await readCsv("./test/data/iris.csv");

// Very Small DataFrames
export let verySmallDataFrame1 = (
    new DataFrame(
        [
            {'id': 1, 'name': 'billy'},
            {'id': 2, 'name': 'jane'},
            {'id': 3, 'name': 'roger'}
        ],
        ['id', 'name']
    )
);

export let verySmallDataFrame2 = (
    new DataFrame(
        [
            {'id': 1, 'name': 'billy', 'age': 10},
            {'id': 2, 'name': 'jane', 'age': 20},
            {'id': 4, 'name': 'gary', 'age': 40}
        ],
        ['id', 'name', 'age']
    )
);

export let verySmallDataFrame3 = (
    new DataFrame(
        [
            {'idCol': 1, 'name': 'billy', 'age': 10},
            {'idCol': 2, 'name': 'jane', 'age': 20},
            {'idCol': 4, 'name': 'gary', 'age': 40}
        ],
        ['idCol', 'name', 'age']
    )
);

export let verySmallValidObjArray = [
    {'id': 1, 'name': 'billy'},
    {'id': 2, 'name': 'jane'},
    {'id': 3, 'name': 'roger'}
];

export let verySmallInvalidObjArray = [
    {'id': 1, 'name': 'billy'},
    {'name': 'jane'},
    {'id': 3, 'name': 'roger'}
];


// Small DataFrames
export let smallDataFrame1 = (
    new DataFrame(
        [
            { 'id': 100, 'name': 'billy', 'weight': 102 },
            { 'id': 2, 'name': 'jane', 'weight': 97 },
            { 'id': 5, 'name': 'roger', 'weight': 107 },
            { 'id': 9, 'name': 'gary', 'weight': 87 },
            { 'id': 1, 'name': 'joseph', 'weight': 71 },
            { 'id': 3, 'name': 'jennifer', 'weight': 84 },
            { 'id': 54, 'name': 'wayne', 'weight': 87 },
            { 'id': 78, 'name': 'carl', 'weight': 86 },
            { 'id': 23, 'name': 'fred', 'weight': 62 },
            { 'id': 100, 'name': 'sean', 'weight': 85 },
            { 'id': 17, 'name': 'steven', 'weight': 107 },
            { 'id': 201, 'name': 'alex', 'weight': 95 },
            { 'id': 169, 'name': 'dwayne', 'weight': 99 },
            { 'id': 101, 'name': 'elon', 'weight': 74 },
            { 'id': 45, 'name': 'issac', 'weight': 104 }
          ],
        ['id', 'name', 'weight']
    )
);


// Test result validation datasets

// Cross join of verySmallDataFrame1 with itself
export let verySmallDataFrameCrossJoinResult = (
    new DataFrame(
        [
            { 'id_x': 1, 'name_x': 'billy', 'id_y': 1, 'name_y': 'billy' },
            { 'id_x': 1, 'name_x': 'billy', 'id_y': 2, 'name_y': 'jane' },
            { 'id_x': 1, 'name_x': 'billy', 'id_y': 3, 'name_y': 'roger' },
            { 'id_x': 2, 'name_x': 'jane', 'id_y': 1, 'name_y': 'billy' },
            { 'id_x': 2, 'name_x': 'jane', 'id_y': 2, 'name_y': 'jane' },
            { 'id_x': 2, 'name_x': 'jane', 'id_y': 3, 'name_y': 'roger' },
            { 'id_x': 3, 'name_x': 'roger', 'id_y': 1, 'name_y': 'billy' },
            { 'id_x': 3, 'name_x': 'roger', 'id_y': 2, 'name_y': 'jane' },
            { 'id_x': 3, 'name_x': 'roger', 'id_y': 3, 'name_y': 'roger' }
        ],
        ['id_x', 'name_x', 'id_y', 'name_y']
    )
);

// Inner join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameInnerJoinResult = (
    new DataFrame(
        [
            { 'id': 1, 'name': 'billy', 'age': 10 },
            { 'id': 2, 'name': 'jane', 'age': 20 }
        ],
        ['id', 'name', 'age']
    )
);

// Left join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameLeftJoinResult = (
    new DataFrame(
        [
            { 'id': 1, 'name': 'billy', 'age': 10 },
            { 'id': 2, 'name': 'jane', 'age': 20 },
            { 'id': 3, 'name': 'roger', 'age': null }
        ],
        ['id', 'name', 'age']
    )
);

// Right join of verySmallDataFrame1 and verySmallDataFrame2
export let verySmallDataFrameRightJoinResult = (
    new DataFrame(
        [
            { 'id': 1, 'name': 'billy', 'age': 10 },
            { 'id': 2, 'name': 'jane', 'age': 20 },
            { 'id': 4, 'name': 'gary', 'age': 40 }
        ],
        ['id', 'name', 'age']
    )
);

// orderBy of smallDataFrame1 by id
export let smallDataFrame1OrderByIdResult = (
    new DataFrame(
        [
            { 'id': 1, 'name': 'joseph', 'weight': 71 },
            { 'id': 2, 'name': 'jane', 'weight': 97 },
            { 'id': 3, 'name': 'jennifer', 'weight': 84 },
            { 'id': 5, 'name': 'roger', 'weight': 107 },
            { 'id': 9, 'name': 'gary', 'weight': 87 },
            { 'id': 17, 'name': 'steven', 'weight': 107 },
            { 'id': 23, 'name': 'fred', 'weight': 62 },
            { 'id': 45, 'name': 'issac', 'weight': 104 },
            { 'id': 54, 'name': 'wayne', 'weight': 87 },
            { 'id': 78, 'name': 'carl', 'weight': 86 },
            { 'id': 100, 'name': 'billy', 'weight': 102 },
            { 'id': 100, 'name': 'sean', 'weight': 85 },
            { 'id': 101, 'name': 'elon', 'weight': 74 },
            { 'id': 169, 'name': 'dwayne', 'weight': 99 },
            { 'id': 201, 'name': 'alex', 'weight': 95 }
        ],
        ['id', 'name', 'weight']
    )
);

// orderBy of smallDataFrame1 by name
export let smallDataFrame1OrderByNameResult = (
    new DataFrame(
        [
            { 'id': 201, 'name': 'alex', 'weight': 95 },
            { 'id': 100, 'name': 'billy', 'weight': 102 },
            { 'id': 78, 'name': 'carl', 'weight': 86 },
            { 'id': 169, 'name': 'dwayne', 'weight': 99 },
            { 'id': 101, 'name': 'elon', 'weight': 74 },
            { 'id': 23, 'name': 'fred', 'weight': 62 },
            { 'id': 9, 'name': 'gary', 'weight': 87 },
            { 'id': 45, 'name': 'issac', 'weight': 104 },
            { 'id': 2, 'name': 'jane', 'weight': 97 },
            { 'id': 3, 'name': 'jennifer', 'weight': 84 },
            { 'id': 1, 'name': 'joseph', 'weight': 71 },
            { 'id': 5, 'name': 'roger', 'weight': 107 },
            { 'id': 100, 'name': 'sean', 'weight': 85 },
            { 'id': 17, 'name': 'steven', 'weight': 107 },
            { 'id': 54, 'name': 'wayne', 'weight': 87 }
        ],
        ['id', 'name', 'weight']
    )
);

// orderBy of smallDataFrame1 by id, weight
export let smallDataFrame1OrderByIdWeightResult = (
    new DataFrame(
        [
            { 'id': 1, 'name': 'joseph', 'weight': 71 },
            { 'id': 2, 'name': 'jane', 'weight': 97 },
            { 'id': 3, 'name': 'jennifer', 'weight': 84 },
            { 'id': 5, 'name': 'roger', 'weight': 107 },
            { 'id': 9, 'name': 'gary', 'weight': 87 },
            { 'id': 17, 'name': 'steven', 'weight': 107 },
            { 'id': 23, 'name': 'fred', 'weight': 62 },
            { 'id': 45, 'name': 'issac', 'weight': 104 },
            { 'id': 54, 'name': 'wayne', 'weight': 87 },
            { 'id': 78, 'name': 'carl', 'weight': 86 },
            { 'id': 100, 'name': 'sean', 'weight': 85 },
            { 'id': 100, 'name': 'billy', 'weight': 102 },
            { 'id': 101, 'name': 'elon', 'weight': 74 },
            { 'id': 169, 'name': 'dwayne', 'weight': 99 },
            { 'id': 201, 'name': 'alex', 'weight': 95 }
        ],
        ['id', 'name', 'weight']
    )
);

// orderBy of smallDataFrame1 by id (desc), weight (asc)
export let smallDataFrame1OrderByIdDescWeightAscResult = (
    new DataFrame(
        [
            { 'id': 201, 'name': 'alex', 'weight': 95 },
            { 'id': 169, 'name': 'dwayne', 'weight': 99 },
            { 'id': 101, 'name': 'elon', 'weight': 74 },
            { 'id': 100, 'name': 'sean', 'weight': 85 },
            { 'id': 100, 'name': 'billy', 'weight': 102 },
            { 'id': 78, 'name': 'carl', 'weight': 86 },
            { 'id': 54, 'name': 'wayne', 'weight': 87 },
            { 'id': 45, 'name': 'issac', 'weight': 104 },
            { 'id': 23, 'name': 'fred', 'weight': 62 },
            { 'id': 17, 'name': 'steven', 'weight': 107 },
            { 'id': 9, 'name': 'gary', 'weight': 87 },
            { 'id': 5, 'name': 'roger', 'weight': 107 },
            { 'id': 3, 'name': 'jennifer', 'weight': 84 },
            { 'id': 2, 'name': 'jane', 'weight': 97 },
            { 'id': 1, 'name': 'joseph', 'weight': 71 }
        ],
        ['id', 'name', 'weight']
    )
);

// groupBy of iris by
export let irisGroupBySpeciesResult = (
    new DataFrame(
        [
            {
              species: 'setosa',
              sepal_length_min: 4.3,
              sepal_length_max: 5.8,
              sepal_length_mean: 5.005999999999999,
              sepal_length_count: 50,
              sepal_length_sum: 250.29999999999998
            },
            {
              species: 'versicolor',
              sepal_length_min: 4.9,
              sepal_length_max: 7,
              sepal_length_mean: 5.936,
              sepal_length_count: 50,
              sepal_length_sum: 296.8
            },
            {
              species: 'virginica',
              sepal_length_min: 4.9,
              sepal_length_max: 7.9,
              sepal_length_mean: 6.587999999999998,
              sepal_length_count: 50,
              sepal_length_sum: 329.3999999999999
            }
        ],
        [
            'species',
            'sepal_length_min',
            'sepal_length_max',
            'sepal_length_mean',
            'sepal_length_count',
            'sepal_length_sum'
        ]
    )
);




//  filter result 1
export let smallDataFrame1FilterIdResult = (
    new DataFrame(
        [
            { 'id': 100, 'name': 'billy', 'weight': 102 },
            { 'id': 100, 'name': 'sean', 'weight': 85 }
        ],
        ['id', 'name', 'weight']
    )
);

export let smallDataFrame1FilterWeightResult = (
    new DataFrame(
        [
            { 'id': 1, 'name': 'joseph', 'weight': 71 },
            { 'id': 23, 'name': 'fred', 'weight': 62 },
            { 'id': 101, 'name': 'elon', 'weight': 74 }
        ],
        ['id', 'name', 'weight']
    )
);
