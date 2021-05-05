import { DataFrame } from '../src/dataframe.js';

export let verySmallDataFrame = (
    new DataFrame(
        [
            {'id': 1, 'name': 'billy'},
            {'id': 2, 'name': 'jane'},
            {'id': 3, 'name': 'roger'}
        ],
        ['id', 'name']
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
)
