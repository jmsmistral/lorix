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
