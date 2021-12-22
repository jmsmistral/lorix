import chai from "chai";
const { expect } = chai;

import {
    verySmallDataFrame1,
    verySmallDataFrame2,
    verySmallDataFrame4,

    verySmallValidObjArray,
    verySmallInvalidObjArray,
    verySmallDataFrameCrossJoinResult,
    verySmallDataFrameInnerJoinResult,
    verySmallDataFrameLeftJoinResult,
    verySmallDataFrameRightJoinResult,
    verySmallDataFrameLeftAntiJoinResult,
    verySmallDataFrameRightAntiJoinResult,
    verySmallDataFrameFullOuterJoinResult,

    smallDataFrame1,
    smallDataFrame2,
    smallDataFrame4,

    smallDataFrame1OrderByIdResult,
    smallDataFrame1OrderByNameResult,
    smallDataFrame1OrderByIdWeightResult,
    smallDataFrame1OrderByIdDescWeightAscResult,

    smallDataFrame1FilterIdResult,
    smallDataFrame1FilterWeightResult,

    smallDataFrame2DistinctAllResult,
    smallDataFrame2DistinctIdResult,
    smallDataFrame2DistinctNameWeightResult,

    smallDataFrame4ReplaceName,
    smallDataFrame4ReplaceNameColour,
    smallDataFrame4ReplaceAllName,
    smallDataFrame4ReplaceAllNameColour,

    smallDataFrame4RegexReplaceName,
    smallDataFrame4RegexReplaceNameGlobal,
    smallDataFrame4RegexReplaceNameColourGlobal,

    iris,
    irisGroupBySpeciesResult,
} from "./sample_data.js"

import { DataFrame } from "../src/dataframe.js";


describe("DataFrame class", () => {

    describe("Constructor", () => {

        it("Should have rows and columns properties.", () => {
            const df = new DataFrame();
            expect(df).to.have.property('rows');
            expect(df).to.have.property('columns');
        });

        it("Should create an empty DataFrame when no parameters are passed.", () => {
            const df = new DataFrame();
            expect(df.rows.length).to.equal(0);
            expect(df.columns.length).to.equal(0);
        });

        it("Should create DataFrame with populated rows and columns properties when parameters are passed.", () => {
            const df = verySmallDataFrame1;
            expect(df.rows.length).to.equal(3);
            expect(df.columns.length).to.equal(2);
        });

    });

    describe("Row iteration", () => {

        it("Should allow iteration over DataFrame rows in a for...of loop", () => {
            const df = verySmallDataFrame1;
            let rowChecker;
            for (const row of df) {
                rowChecker = row;
            }
            // rowChecker should be set to the last row
            expect(rowChecker["id"]).to.equal(verySmallDataFrame1.slice(-1).toArray()[0]["id"]);
        });

        it("Should allow destructuring DataFrame rows", () => {
            const df = [...verySmallDataFrame1];
            // Compare the
            expect(df.slice(-1)[0]["id"]).to.equal(verySmallDataFrame1.slice(-1).toArray()[0]["id"]);
        });

    });

    describe("select()", function() {

        beforeEach(function() {
            this.currentTest.df = verySmallDataFrame1;
        });

        it("Should throw an error if no columns are selected", function() {
            expect(() => {this.test.df.select()}).to.throw();
        });

        it("Should throw an error if at least one column does not exist in the DataFrame", function() {
            expect(() => {this.test.df.select("id", "colthatdoesnotexist")}).to.throw();
        });

        it("Should return a new DataFrame with only the selected columns", function() {
            const df = this.test.df.select("name");
            expect(df.columns).to.have.all.members(["name"]);
            expect(df.columns).to.not.have.all.members(["id"]);
            for (let row of df) {
                expect(row).to.haveOwnProperty("name");
                expect(row).not.to.haveOwnProperty("id");
            }
        });

    });

    describe("drop()", function() {

        beforeEach(function() {
            this.currentTest.df = verySmallDataFrame1;
        });

        it("Should throw an error if no columns are dropped", function() {
            expect(() => {this.test.df.drop()}).to.throw();
        });

        it("Should throw an error if at least one column does not exist in the DataFrame", function() {
            expect(() => {this.test.df.drop("id", "colthatdoesnotexist")}).to.throw();
        });

        it("Should return a new DataFrame excluding the dropped columns", function() {
            const df = this.test.df.drop("name");
            expect(df.columns).to.have.all.members(["id"]);
            expect(df.columns).to.not.have.all.members(["name"]);
            for (let row of df) {
                expect(row).to.haveOwnProperty("id");
                expect(row).not.to.haveOwnProperty("name");
            }
        });

    });

    describe("withColumn()", function() {

        beforeEach(function() {
            this.currentTest.df = verySmallDataFrame1;
        });

        it("Should throw an error if column name is invalid", function() {
            // Is not a string
            expect(() => {this.test.df.withColumn(1, () => 1)}).to.throw();
            expect(() => {this.test.df.withColumn(undefined, () => 1)}).to.throw();
            expect(() => {this.test.df.withColumn(null, () => 1)}).to.throw();
            // Starts with a number
            expect(() => {this.test.df.withColumn("1newCol", () => 1)}).to.throw();
            // Starts with punctuation other than underscore
            ["-", "~", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")",
             "[", "]", "+", "=", "{", "}", ":", ";", "'", "|", "\\", "/",
             "<", ">", ",", ".", "?"].forEach((invalidChar) => {
                expect(() => {this.test.df.withColumn(invalidChar + "newCol", () => 1)}).to.throw();
            });
        });

        it("Should add a new column when a non-existing valid column name is specified", function() {
            const df = this.test.df.withColumn("newCol", () => 1);
            expect(df.columns.includes("newCol")).to.be.true;
            for (let row of df) {
                expect(row).to.haveOwnProperty("newCol");
                expect(row["newCol"]).to.equal(1);
            }
        });

        it("Should overwrite the values of a column when an existing column name is specified", function() {
            const newColName = "newCol";
            const df = (
                this.test.df
                .withColumn(newColName, () => 1)
                .withColumn(newColName, () => 2)
            );

            const numInstances = df.columns.filter((col) => col == newColName).length;

            expect(numInstances).to.equal(1);  // There shouldn't be more than one instance of a column name
            expect(df.columns.includes(newColName)).to.be.true;
            for (let row of df) {
                expect(row).to.haveOwnProperty(newColName);
                expect(row[newColName]).to.equal(2);
            }
        });

        it("Should throw an error if no definition is provided for the new column", function() {
            expect(() => {this.test.df.withColumn("newCol")}).to.throw();
        });

        it("Should throw an error when a function is not passed as the column definition", function() {
            expect(() => {this.test.df.withColumn("newCol", 1)}).to.throw();
        });

        it("Should allow for existing columns to be referenced in the column defintion", function() {
            const existingCol = this.test.df.columns[0];  // Get the first column in test DataFrame
            const df = this.test.df.withColumn("newCol", (row) => row[existingCol]);
            expect(df.columns.includes("newCol")).to.be.true;
            for (let row of df) {
                expect(row).to.haveOwnProperty("newCol");
                expect(row["newCol"]).to.equal(row[existingCol]);
            }
        });

        it("Should throw an error when referencing a non-existent column", function() {
            expect(() => {this.test.df.withColumn("newCol", (row) => row["nonExistingColumn"])}).to.throw();
        });

    });

    describe("fromArray()", function() {

        beforeEach(function() {
            this.currentTest.validArray = verySmallValidObjArray;
            this.currentTest.invalidArray = verySmallInvalidObjArray;
        });

        it("Should return a DataFrame if a valid array of objects is passed", function() {
            expect(() => DataFrame.fromArray(this.test.validArray)).to.not.throw();
            const df = DataFrame.fromArray(this.test.validArray);
            expect(df.rows.length).to.equal(this.test.validArray.length);
        });

        it("Should throw an error if array is empty", function() {
            expect(() => DataFrame.fromArray([])).to.throw();
        });

        it("Should throw an error if properties are not the same for all objects", function() {
            expect(() => DataFrame.fromArray(this.test.invalidArray)).to.throw();
        });

        it("Should throw an error if an array is not passed", function() {
            expect(() => DataFrame.fromArray(() => "error")).to.throw();
        });

    });

    describe("crossJoin()", function() {
        beforeEach(function() {
            this.currentTest.df1 = verySmallDataFrame1;
            this.currentTest.df2 = verySmallDataFrame1;
            this.currentTest.crossJoinResultDf = verySmallDataFrameCrossJoinResult;
        });

        it("Should return the cross join between two DataFrames", function() {
            let result = this.test.df1.crossJoin(this.test.df2);
            expect(result.toArray()).to.deep.equal(this.test.crossJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.crossJoinResultDf.columns);
        });

        it("Should throw an error if a DataFrame is not passed", function() {
            expect(() => this.test.df1.crossJoin(()=> "should error")).to.throw();
            expect(() => this.test.df1.crossJoin([{"id": 1, "name": "test"}])).to.throw();
        });

        it("Should throw an error if no argument is passed", function() {
            expect(() => this.test.df1.crossJoin()).to.throw();
        });

        it("Should throw an error if more than one argument is passed", function() {
            expect(() => this.test.df1.crossJoin(this.test.df2, (l, r) => l.id == r.id)).to.throw();
        });
    });

    describe("innerJoin()", function() {
        beforeEach(function() {
            this.currentTest.df1 = verySmallDataFrame1;
            this.currentTest.df2 = verySmallDataFrame2;
            this.currentTest.innerJoinResultDf = verySmallDataFrameInnerJoinResult;
        });

        it("Should return the inner join between two DataFrames when using a single array join condition", function() {
            let result = this.test.df1.innerJoin(this.test.df2, ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.innerJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.innerJoinResultDf.columns);
        });

        it("Should return the inner join between two DataFrames when using left and right array join conditions", function() {
            let result = this.test.df1.innerJoin(this.test.df2, ["id", "name"], ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.innerJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.innerJoinResultDf.columns);
        });

        it("Should return the inner join between two DataFrames when using a function join condition", function() {
            let result = this.test.df1.innerJoin(this.test.df2, (l, r) => (l["id"] == r["id"]) && (l["name"] == r["name"]));
            expect(result.toArray()).to.deep.equal(this.test.innerJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.innerJoinResultDf.columns);
        });

        it( "Should throw an error if a DataFrame is not passed", function() {
            expect(() => this.test.df1.innerJoin(()=> "should error"), ["id"]).to.throw();
        });

        it("Should throw an error if no argument is passed", function() {
            expect(() => this.test.df1.innerJoin()).to.throw();
        });

        it("Should throw an error if more than three arguments are passed", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id"], 1, 1)).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a single array join condition", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, ["invalidCol"])).to.throw();
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id", "invalidCol"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using left and right array join conditions", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id"], ["invalidCol"])).to.throw();
            expect(() => this.test.df1.innerJoin(this.test.df2, ["invalidCol"], ["id"])).to.throw();
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id", "invalidCol"], ["id"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a function join condition", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.invalidCol == "test") )).to.throw();
            expect(() => this.test.df1.innerJoin(this.test.df2, (l, r) => (l.id == r.id) || (r.invalidCol == "test") )).to.throw();
        });

        it("Should throw an error if an empty array is passed using a single array join condition", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, [])).to.throw();
        });

        it("Should throw an error if an empty array is passed using left and right array join conditions", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, [], [])).to.throw();
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id"], [])).to.throw();
            expect(() => this.test.df1.innerJoin(this.test.df2, [], ["id"])).to.throw();
        });

        it("Should throw an error if an left and right array join conditions are of different lengths", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id"], ["id", "name"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a function join condition", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.name == "error") )).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a single array join condition", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using left and right array join conditions", function() {
            expect(() => this.test.df1.innerJoin(this.test.df2, ["id"], ["id"])).to.throw();
            expect(() => this.test.df1.innerJoin(this.test.df3, ["id"], ["idCol"])).to.throw();
        });

    });

    describe("leftJoin()", function() {
        beforeEach(function() {
            this.currentTest.df1 = verySmallDataFrame1;
            this.currentTest.df2 = verySmallDataFrame2;
            this.currentTest.leftJoinResultDf = verySmallDataFrameLeftJoinResult;
        });

        it("Should return the left join between two DataFrames when using a single array join condition", function() {
            let result = this.test.df1.leftJoin(this.test.df2, ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.leftJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.leftJoinResultDf.columns);
        });

        it("Should return the left join between two DataFrames when using left and right array join conditions", function() {
            let result = this.test.df1.leftJoin(this.test.df2, ["id", "name"], ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.leftJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.leftJoinResultDf.columns);
        });

        it("Should return the left join between two DataFrames when using a function join condition", function() {
            let result = this.test.df1.leftJoin(this.test.df2, (l, r) => (l["id"] == r["id"]) && (l["name"] == r["name"]));
            expect(result.toArray()).to.deep.equal(this.test.leftJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.leftJoinResultDf.columns);
        });

        it( "Should throw an error if a DataFrame is not passed", function() {
            expect(() => this.test.df1.leftJoin(()=> "should error"), ["id"]).to.throw();
        });

        it("Should throw an error if no argument is passed", function() {
            expect(() => this.test.df1.leftJoin()).to.throw();
        });

        it("Should throw an error if more than three arguments are passed", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id"], 1, 1)).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a single array join condition", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, ["invalidCol"])).to.throw();
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id", "invalidCol"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using left and right array join conditions", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id"], ["invalidCol"])).to.throw();
            expect(() => this.test.df1.leftJoin(this.test.df2, ["invalidCol"], ["id"])).to.throw();
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id", "invalidCol"], ["id"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a function join condition", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.invalidCol == "test") )).to.throw();
            expect(() => this.test.df1.leftJoin(this.test.df2, (l, r) => (l.id == r.id) || (r.invalidCol == "test") )).to.throw();
        });

        it("Should throw an error if an empty array is passed using a single array join condition", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, [])).to.throw();
        });

        it("Should throw an error if an empty array is passed using left and right array join conditions", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, [], [])).to.throw();
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id"], [])).to.throw();
            expect(() => this.test.df1.leftJoin(this.test.df2, [], ["id"])).to.throw();
        });

        it("Should throw an error if an left and right array join conditions are of different lengths", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id"], ["id", "name"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a function join condition", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.name == "error") )).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a single array join condition", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using left and right array join conditions", function() {
            expect(() => this.test.df1.leftJoin(this.test.df2, ["id"], ["id"])).to.throw();
            expect(() => this.test.df1.leftJoin(this.test.df3, ["id"], ["idCol"])).to.throw();
        });

    });

    describe("rightJoin()", function() {
        beforeEach(function() {
            this.currentTest.df1 = verySmallDataFrame1;
            this.currentTest.df2 = verySmallDataFrame2;
            this.currentTest.rightJoinResultDf = verySmallDataFrameRightJoinResult;
        });

        it("Should return the right join between two DataFrames when using a single array join condition", function() {
            let result = this.test.df1.rightJoin(this.test.df2, ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.rightJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.rightJoinResultDf.columns);
        });

        it("Should return the right join between two DataFrames when using right and right array join conditions", function() {
            let result = this.test.df1.rightJoin(this.test.df2, ["id", "name"], ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.rightJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.rightJoinResultDf.columns);
        });

        it("Should return the right join between two DataFrames when using a function join condition", function() {
            let result = this.test.df1.rightJoin(this.test.df2, (l, r) => (l["id"] == r["id"]) && (l["name"] == r["name"]));
            expect(result.toArray()).to.deep.equal(this.test.rightJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.rightJoinResultDf.columns);
        });

        it( "Should throw an error if a DataFrame is not passed", function() {
            expect(() => this.test.df1.rightJoin(()=> "should error"), ["id"]).to.throw();
        });

        it("Should throw an error if no argument is passed", function() {
            expect(() => this.test.df1.rightJoin()).to.throw();
        });

        it("Should throw an error if more than three arguments are passed", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id"], 1, 1)).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a single array join condition", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, ["invalidCol"])).to.throw();
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id", "invalidCol"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using right and right array join conditions", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id"], ["invalidCol"])).to.throw();
            expect(() => this.test.df1.rightJoin(this.test.df2, ["invalidCol"], ["id"])).to.throw();
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id", "invalidCol"], ["id"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a function join condition", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.invalidCol == "test") )).to.throw();
            expect(() => this.test.df1.rightJoin(this.test.df2, (l, r) => (l.id == r.id) || (r.invalidCol == "test") )).to.throw();
        });

        it("Should throw an error if an empty array is passed using a single array join condition", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, [])).to.throw();
        });

        it("Should throw an error if an empty array is passed using right and right array join conditions", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, [], [])).to.throw();
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id"], [])).to.throw();
            expect(() => this.test.df1.rightJoin(this.test.df2, [], ["id"])).to.throw();
        });

        it("Should throw an error if an right and right array join conditions are of different lengths", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id"], ["id", "name"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a function join condition", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.name == "error") )).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a single array join condition", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using right and right array join conditions", function() {
            expect(() => this.test.df1.rightJoin(this.test.df2, ["id"], ["id"])).to.throw();
            expect(() => this.test.df1.rightJoin(this.test.df3, ["id"], ["idCol"])).to.throw();
        });

    });

    describe("leftAntiJoin()", function() {
        beforeEach(function() {
            this.currentTest.df1 = verySmallDataFrame1;
            this.currentTest.df2 = verySmallDataFrame2;
            this.currentTest.leftAntiJoinResultDf = verySmallDataFrameLeftAntiJoinResult;
        });

        it("Should return the left anti join between two DataFrames when using a single array join condition", function() {
            let result = this.test.df1.leftAntiJoin(this.test.df2, ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.leftAntiJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.leftAntiJoinResultDf.columns);
        });

        it("Should return the left anti join between two DataFrames when using left and right array join conditions", function() {
            let result = this.test.df1.leftAntiJoin(this.test.df2, ["id", "name"], ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.leftAntiJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.leftAntiJoinResultDf.columns);
        });

        it("Should return the left anti join between two DataFrames when using a function join condition", function() {
            let result = this.test.df1.leftAntiJoin(this.test.df2, (l, r) => (l["id"] == r["id"]) && (l["name"] == r["name"]));
            expect(result.toArray()).to.deep.equal(this.test.leftAntiJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.leftAntiJoinResultDf.columns);
        });

        it( "Should throw an error if a DataFrame is not passed", function() {
            expect(() => this.test.df1.leftAntiJoin(()=> "should error"), ["id"]).to.throw();
        });

        it("Should throw an error if no argument is passed", function() {
            expect(() => this.test.df1.leftAntiJoin()).to.throw();
        });

        it("Should throw an error if more than three arguments are passed", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id"], 1, 1)).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a single array join condition", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["invalidCol"])).to.throw();
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id", "invalidCol"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using left and right array join conditions", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id"], ["invalidCol"])).to.throw();
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["invalidCol"], ["id"])).to.throw();
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id", "invalidCol"], ["id"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a function join condition", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.invalidCol == "test") )).to.throw();
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, (l, r) => (l.id == r.id) || (r.invalidCol == "test") )).to.throw();
        });

        it("Should throw an error if an empty array is passed using a single array join condition", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, [])).to.throw();
        });

        it("Should throw an error if an empty array is passed using left and right array join conditions", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, [], [])).to.throw();
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id"], [])).to.throw();
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, [], ["id"])).to.throw();
        });

        it("Should throw an error if an left and right array join conditions are of different lengths", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id"], ["id", "name"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a function join condition", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.name == "error") )).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a single array join condition", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using left and right array join conditions", function() {
            expect(() => this.test.df1.leftAntiJoin(this.test.df2, ["id"], ["id"])).to.throw();
            expect(() => this.test.df1.leftAntiJoin(this.test.df3, ["id"], ["idCol"])).to.throw();
        });

    });

    describe("rightAntiJoin()", function() {
        beforeEach(function() {
            this.currentTest.df1 = verySmallDataFrame1;
            this.currentTest.df2 = verySmallDataFrame2;
            this.currentTest.rightAntiJoinResultDf = verySmallDataFrameRightAntiJoinResult;
        });

        it("Should return the right anti join between two DataFrames when using a single array join condition", function() {
            let result = this.test.df1.rightAntiJoin(this.test.df2, ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.rightAntiJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.rightAntiJoinResultDf.columns);
        });

        it("Should return the right anti join between two DataFrames when using right and right array join conditions", function() {
            let result = this.test.df1.rightAntiJoin(this.test.df2, ["id", "name"], ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.rightAntiJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.rightAntiJoinResultDf.columns);
        });

        it("Should return the right anti join between two DataFrames when using a function join condition", function() {
            let result = this.test.df1.rightAntiJoin(this.test.df2, (l, r) => (l["id"] == r["id"]) && (l["name"] == r["name"]));
            expect(result.toArray()).to.deep.equal(this.test.rightAntiJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.rightAntiJoinResultDf.columns);
        });

        it( "Should throw an error if a DataFrame is not passed", function() {
            expect(() => this.test.df1.rightAntiJoin(()=> "should error"), ["id"]).to.throw();
        });

        it("Should throw an error if no argument is passed", function() {
            expect(() => this.test.df1.rightAntiJoin()).to.throw();
        });

        it("Should throw an error if more than three arguments are passed", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id"], 1, 1)).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a single array join condition", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["invalidCol"])).to.throw();
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id", "invalidCol"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using right and right array join conditions", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id"], ["invalidCol"])).to.throw();
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["invalidCol"], ["id"])).to.throw();
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id", "invalidCol"], ["id"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a function join condition", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.invalidCol == "test") )).to.throw();
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, (l, r) => (l.id == r.id) || (r.invalidCol == "test") )).to.throw();
        });

        it("Should throw an error if an empty array is passed using a single array join condition", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, [])).to.throw();
        });

        it("Should throw an error if an empty array is passed using right and right array join conditions", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, [], [])).to.throw();
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id"], [])).to.throw();
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, [], ["id"])).to.throw();
        });

        it("Should throw an error if an right and right array join conditions are of different lengths", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id"], ["id", "name"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a function join condition", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.name == "error") )).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a single array join condition", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using right and right array join conditions", function() {
            expect(() => this.test.df1.rightAntiJoin(this.test.df2, ["id"], ["id"])).to.throw();
            expect(() => this.test.df1.rightAntiJoin(this.test.df3, ["id"], ["idCol"])).to.throw();
        });

    });

    describe("fullOuterJoin()", function() {
        beforeEach(function() {
            this.currentTest.df1 = verySmallDataFrame2;
            this.currentTest.df2 = verySmallDataFrame4;
            this.currentTest.fullOuterJoinResultDf = verySmallDataFrameFullOuterJoinResult;
        });

        it("Should return the full outer join between two DataFrames when using a single array join condition", function() {
            let result = this.test.df1.fullOuterJoin(this.test.df2, ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.fullOuterJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.fullOuterJoinResultDf.columns);
        });

        it("Should return the full outer join between two DataFrames when using right and right array join conditions", function() {
            let result = this.test.df1.fullOuterJoin(this.test.df2, ["id", "name"], ["id", "name"]);
            expect(result.toArray()).to.deep.equal(this.test.fullOuterJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.fullOuterJoinResultDf.columns);
        });

        it("Should return the full outer join between two DataFrames when using a function join condition", function() {
            let result = this.test.df1.fullOuterJoin(this.test.df2, (l, r) => (l["id"] == r["id"]) && (l["name"] == r["name"]));
            expect(result.toArray()).to.deep.equal(this.test.fullOuterJoinResultDf.toArray());
            expect(result.columns).to.deep.equal(this.test.fullOuterJoinResultDf.columns);
        });

        it( "Should throw an error if a DataFrame is not passed", function() {
            expect(() => this.test.df1.fullOuterJoin(()=> "should error"), ["id"]).to.throw();
        });

        it("Should throw an error if no argument is passed", function() {
            expect(() => this.test.df1.fullOuterJoin()).to.throw();
        });

        it("Should throw an error if more than three arguments are passed", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id"], 1, 1)).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a single array join condition", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["invalidCol"])).to.throw();
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id", "invalidCol"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using right and right array join conditions", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id"], ["invalidCol"])).to.throw();
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["invalidCol"], ["id"])).to.throw();
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id", "invalidCol"], ["id"])).to.throw();
        });

        it("Should throw an error if a non-existent column is passed using a function join condition", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.invalidCol == "test") )).to.throw();
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, (l, r) => (l.id == r.id) || (r.invalidCol == "test") )).to.throw();
        });

        it("Should throw an error if an empty array is passed using a single array join condition", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, [])).to.throw();
        });

        it("Should throw an error if an empty array is passed using right and right array join conditions", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, [], [])).to.throw();
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id"], [])).to.throw();
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, [], ["id"])).to.throw();
        });

        it("Should throw an error if an right and right array join conditions are of different lengths", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id"], ["id", "name"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a function join condition", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, (l, r) => (l.id == r.id) && (r.name == "error") )).to.throw();
        });

        it("Should throw an error if there are overlapping columns using a single array join condition", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id"])).to.throw();
        });

        it("Should throw an error if there are overlapping columns using right and right array join conditions", function() {
            expect(() => this.test.df1.fullOuterJoin(this.test.df2, ["id"], ["id"])).to.throw();
            expect(() => this.test.df1.fullOuterJoin(this.test.df3, ["id"], ["idCol"])).to.throw();
        });

    });

    describe("orderBy()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame1;
            this.currentTest.orderByIdResultDf = smallDataFrame1OrderByIdResult;
            this.currentTest.orderByNameResultDf = smallDataFrame1OrderByNameResult;
            this.currentTest.orderByIdWeightResultDf = smallDataFrame1OrderByIdWeightResult;
            this.currentTest.orderByIdWeightDescResultDf = smallDataFrame1OrderByIdDescWeightAscResult;
        });

        it("Should return a new DataFrame ordered by the specified columns and sort order", function() {
            let result1 = this.test.df.orderBy(["id"]);
            expect(result1.toArray()).to.deep.equal(this.test.orderByIdResultDf.toArray());
            expect(result1.columns).to.deep.equal(this.test.orderByIdResultDf.columns);

            let result2 = this.test.df.orderBy(["name"]);
            expect(result2.toArray()).to.deep.equal(this.test.orderByNameResultDf.toArray());
            expect(result2.columns).to.deep.equal(this.test.orderByNameResultDf.columns);

            let result3 = this.test.df.orderBy(["id", "weight"]);
            expect(result3.toArray()).to.deep.equal(this.test.orderByIdWeightResultDf.toArray());
            expect(result3.columns).to.deep.equal(this.test.orderByIdWeightResultDf.columns);

            let result4 = this.test.df.orderBy(["id", "weight"], ["desc", "asc"]);
            expect(result4.toArray()).to.deep.equal(this.test.orderByIdWeightDescResultDf.toArray());
            expect(result4.columns).to.deep.equal(this.test.orderByIdWeightDescResultDf.columns);
        });

        it("Should throw an error if no columns are specified", function() {
            expect(() => this.test.df.orderBy([])).to.throw();
        });

        it("Should throw an error if an array is not passed", function() {
            expect(() => this.test.df.orderBy("notAnArray")).to.throw();
        });

        it("Should throw an error if an array is not passed for sort order", function() {
            expect(() => this.test.df.orderBy(["id"], "notAnArray")).to.throw();
        });

        it("Should throw an error if no valid columns are specified", function() {
            expect(() => this.test.df.orderBy(["invalidColumn"])).to.throw();
        });

        it("Should throw an error if no valid values are specified for sort order", function() {
            expect(() => this.test.df.orderBy(["id"], ["invalidValue"])).to.throw();
        });

    });

    describe("groupBy()", function() {

        beforeEach(function() {
            this.currentTest.df = iris;
            this.currentTest.groupBySpeciesResultDf = irisGroupBySpeciesResult;
        });

        it("Should return a new DataFrame grouped by the specified columns with the specified aggregations", function() {
            let result1 = (
                this.test.df
                .groupBy(
                    ["species"],
                    {
                        "sepal_length": ["min", "max", "mean", "count", "sum"]
                    }
                )
            );
            expect(result1.toArray()).to.deep.equal(this.test.groupBySpeciesResultDf.toArray());
            expect(result1.columns).to.deep.equal(this.test.groupBySpeciesResultDf.columns);
        });

        it("Should throw an error if no columns are specified", function() {
            expect(() => this.test.df.groupBy([])).to.throw();
        });

        it("Should throw an error if an array is not passed", function() {
            expect(() => this.test.df.groupBy("notAnArray")).to.throw();
        });

        it("Should throw an error if no valid columns are specified", function() {
            expect(() => this.test.df.groupBy(["invalidColumn"])).to.throw();
        });

        it("Should throw an error if no valid values are specified for aggregation", function() {
            expect(() => this.test.df.groupBy(
                ["species"],
                {"sepal_length": ["min", "max", "invalidAgg"]}
            )).to.throw();
        });

    });

    describe("filter()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame1;
            this.currentTest.filterIdResultDf = smallDataFrame1FilterIdResult;
            this.currentTest.filterWeightResultDf = smallDataFrame1FilterWeightResult;
        });

        it("Should return a new DataFrame filtered by the specified columns", function() {
            let resultId = this.test.df.filter((row) => row["id"] == 100);
            let resultWeight = this.test.df.filter((row) => row["weight"] < 80);

            // Filter based on id
            expect(resultId.toArray()).to.deep.equal(this.test.filterIdResultDf.toArray());
            expect(resultId.columns).to.deep.equal(this.test.filterIdResultDf.columns);

            // Filter based on weight
            expect(resultWeight.toArray()).to.deep.equal(this.test.filterWeightResultDf.toArray());
            expect(resultWeight.columns).to.deep.equal(this.test.filterWeightResultDf.columns);
        });

        it("Should throw an error if no function is specified", function() {
            expect(() => this.test.df.filter()).to.throw();
        });

        it("Should throw an error when a type other than a function is passed", function() {
            expect(() => {this.test.df.filter(1)}).to.throw();
            expect(() => {this.test.df.filter("stringInsteadOfFunction")}).to.throw();
        });

        it("Should throw an error when referencing a non-existent column", function() {
            expect(() => {this.test.df.filter((row) => row["nonExistingColumn"] > 1)}).to.throw();
        });

        it("Should throw an error if more than one argument is passed", function() {
            expect(() => {this.test.df.filter(((row) => row["weight"] < 80), "test")}).to.throw();
        });

    });


    describe("distinct()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame2;

            this.currentTest.distinctAllResultDf = smallDataFrame2DistinctAllResult;
            this.currentTest.distinctIdResultDf = smallDataFrame2DistinctIdResult;
            this.currentTest.distinctNameWeightResultDf = smallDataFrame2DistinctNameWeightResult;
        });

        it("Should return a new DataFrame with duplicate rows dropped as per the specified columns", function() {
            let resultAll = this.test.df.distinct();
            let resultId = this.test.df.distinct(["id"]);
            let resultNameWeight = this.test.df.distinct(["name", "weight"]);

            // Distinct based on all columns
            expect(resultAll.toArray()).to.deep.equal(this.test.distinctAllResultDf.toArray());
            expect(resultAll.columns).to.deep.equal(this.test.distinctAllResultDf.columns);

            // Distinct based on id
            expect(resultId.toArray()).to.deep.equal(this.test.distinctIdResultDf.toArray());
            expect(resultId.columns).to.deep.equal(this.test.distinctIdResultDf.columns);

            // Distinct based on name and weight
            expect(resultNameWeight.toArray()).to.deep.equal(this.test.distinctNameWeightResultDf.toArray());
            expect(resultNameWeight.columns).to.deep.equal(this.test.distinctNameWeightResultDf.columns);
        });

        it("Should throw an error when a type other than an array is passed", function() {
            expect(() => {this.test.df.distinct(1)}).to.throw();
            expect(() => {this.test.df.distinct("stringInsteadOfFunction")}).to.throw();
        });

        it("Should throw an error when referencing a non-existent column", function() {
            expect(() => {this.test.df.distinct(["nonExistingColumn"])}).to.throw();
        });

        it("Should throw an error if more than one argument is passed", function() {
            expect(() => {this.test.df.distinct(["name", "weight"], "anotherArgument")}).to.throw();
        });

    });


    describe("replace()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame4;

            this.currentTest.replaceNameResultDf = smallDataFrame4ReplaceName;
            this.currentTest.replaceNameColourResultDf = smallDataFrame4ReplaceNameColour;
        });

        it("Should return a new DataFrame with values correctly replaced across the specified columns", function() {
            let resultName = this.test.df.replace(["name"], "r", "rrr");
            let resultNameColour = this.test.df.replace(["name", "colour"], "r", "rrr");

            expect(resultName.toArray()).to.deep.equal(this.test.replaceNameResultDf.toArray());
            expect(resultName.columns).to.deep.equal(this.test.replaceNameResultDf.columns);

            expect(resultNameColour.toArray()).to.deep.equal(this.test.replaceNameColourResultDf.toArray());
            expect(resultNameColour.columns).to.deep.equal(this.test.replaceNameColourResultDf.columns);
        });

        it("Should throw an error when a type other than an array is passed as first parameter", function() {
            expect(() => {this.test.df.replace(1, "r", "rrr")}).to.throw();
        });

        it("Should throw an error when a type other than a string is passed for first and second parameters", function() {
            expect(() => {this.test.df.replace(["name"], 1, "rrr")}).to.throw();
            expect(() => {this.test.df.replace(["name"], "r", 1)}).to.throw();
        });

        it("Should throw an error when referencing a non-existent column", function() {
            expect(() => {this.test.df.replace(["nonExistentColumn"], "r", "rrr");}).to.throw();
        });

        it("Should throw an error if an incorrect number of arguments are passed", function() {
            expect(() => {this.test.df.replace();}).to.throw();
            expect(() => {this.test.df.replace(["name"], "r", "rrr", 1);}).to.throw();
        });

    });


    describe("replaceAll()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame4;

            this.currentTest.replaceAllNameResultDf = smallDataFrame4ReplaceAllName;
            this.currentTest.replaceAllNameColourResultDf = smallDataFrame4ReplaceAllNameColour;
        });

        it("Should return a new DataFrame with values correctly replaced across the specified columns", function() {
            let resultName = this.test.df.replaceAll(["name"], "r", "rrr");
            let resultNameColour = this.test.df.replaceAll(["name", "colour"], "r", "rrr");

            expect(resultName.toArray()).to.deep.equal(this.test.replaceAllNameResultDf.toArray());
            expect(resultName.columns).to.deep.equal(this.test.replaceAllNameResultDf.columns);

            expect(resultNameColour.toArray()).to.deep.equal(this.test.replaceAllNameColourResultDf.toArray());
            expect(resultNameColour.columns).to.deep.equal(this.test.replaceAllNameColourResultDf.columns);
        });

        it("Should throw an error when a type other than an array is passed as first parameter", function() {
            expect(() => {this.test.df.replaceAll(1, "r", "rrr")}).to.throw();
        });

        it("Should throw an error when a type other than a string is passed for first and second parameters", function() {
            expect(() => {this.test.df.replaceAll(["name"], 1, "rrr")}).to.throw();
            expect(() => {this.test.df.replaceAll(["name"], "r", 1)}).to.throw();
        });

        it("Should throw an error when referencing a non-existent column", function() {
            expect(() => {this.test.df.replaceAll(["nonExistentColumn"], "r", "rrr");}).to.throw();
        });

        it("Should throw an error if an incorrect number of arguments are passed", function() {
            expect(() => {this.test.df.replaceAll();}).to.throw();
            expect(() => {this.test.df.replaceAll(["name"], "r", "rrr", 1);}).to.throw();
        });

    });


    describe("regexReplace()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame4;

            this.currentTest.replaceAllNameResultDf = smallDataFrame4RegexReplaceName;
            this.currentTest.replaceAllNameGlobalResultDf = smallDataFrame4RegexReplaceNameGlobal;
            this.currentTest.replaceAllNameColourGlobalResultDf = smallDataFrame4RegexReplaceNameColourGlobal;
        });

        it("Should return a new DataFrame with values correctly replaced across the specified columns", function() {
            let resultName = this.test.df.regexReplace(["name"], /r/i, "rrr");
            let resultNameGlobal = this.test.df.regexReplace(["name"], /r/ig, "rrr");
            let resultNameColourGlobal = this.test.df.regexReplace(["name", "colour"], /r/ig, "rrr");

            expect(resultName.toArray()).to.deep.equal(this.test.replaceAllNameResultDf.toArray());
            expect(resultName.columns).to.deep.equal(this.test.replaceAllNameResultDf.columns);

            expect(resultNameGlobal.toArray()).to.deep.equal(this.test.replaceAllNameGlobalResultDf.toArray());
            expect(resultNameGlobal.columns).to.deep.equal(this.test.replaceAllNameGlobalResultDf.columns);

            expect(resultNameColourGlobal.toArray()).to.deep.equal(this.test.replaceAllNameColourGlobalResultDf.toArray());
            expect(resultNameColourGlobal.columns).to.deep.equal(this.test.replaceAllNameColourGlobalResultDf.columns);
        });

        it("Should throw an error when a type other than an array is passed as first parameter", function() {
            expect(() => {this.test.df.regexReplace(1, /r/i, "rrr")}).to.throw();
        });

        it("Should throw an error when a type other than a string is passed for first and second parameters", function() {
            expect(() => {this.test.df.regexReplace(["name"], 1, "rrr")}).to.throw();
            expect(() => {this.test.df.regexReplace(["name"], /r/i, 1)}).to.throw();
        });

        it("Should throw an error when referencing a non-existent column", function() {
            expect(() => {this.test.df.regexReplace(["nonExistentColumn"], /r/i, "rrr");}).to.throw();
        });

        it("Should throw an error if an incorrect number of arguments are passed", function() {
            expect(() => {this.test.df.regexReplace();}).to.throw();
            expect(() => {this.test.df.regexReplace(["name"], /r/i, "rrr", 1);}).to.throw();
        });

    });

});
