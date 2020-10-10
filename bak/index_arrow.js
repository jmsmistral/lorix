const fs = require('fs');
const path = require('path');
const arrow = require('apache-arrow');
const d3 = require('d3');

const dataPath = path.join(__dirname, '/test.csv');

dataSchema = [
    {name: 'id', type: {name: 'map'}, nullable: true, children: []},
    {name: 'name', type: {name: 'map'}, nullable: true, children: []}
]


fs.readFile(dataPath, 'utf-8', async (err, data) => {
    if (err) throw err;
    console.log(data);
    const parsedData = await d3.csvParse(data, d3.autoType);
    console.log(parsedData);
    console.log(parsedData.length);

    const t = arrow.Table.new(
        [
            arrow.Int32Vector.from(parsedData.map(d => d.id)),
            arrow.Utf8Vector.from(parsedData.map(d => d.name))
        ],
        ['id', 'name']
    );

    // console.log(t.data.childData.map(d => new Uint32Array(d.values).toString()));
    console.log(t.toArray());
    console.log(t.schema.fields.map((field) => field.name));
    console.log(t.schema.toString());
    console.log(t.getColumn('name').get(0));
    console.log(t.countBy('name').toJSON());
    console.log(t.countBy('name').toArray());
});
