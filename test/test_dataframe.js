import chai from "chai";
const { expect } = chai;

import {
    verySmallDataFrame1,
    verySmallDataFrame2,
    verySmallValidObjArray,
    verySmallInvalidObjArray,
    verySmallDataFrameCrossJoinResult,
    verySmallDataFrameInnerJoinResult
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
            const df = (
                this.test.df
                .withColumn("newCol", () => 1)
                .withColumn("newCol", () => 2)
            );
            expect(df.columns.includes("newCol")).to.be.true;
            for (let row of df) {
                expect(row).to.haveOwnProperty("newCol");
                expect(row["newCol"]).to.equal(2);
            }
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

});
