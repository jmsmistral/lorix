import { DataFrame } from '../src/dataframe.js';

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
