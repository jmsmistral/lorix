import chai from "chai";
const { expect } = chai;

import {
    verySmallDataFrame,
    verySmallValidObjArray,
    verySmallInvalidObjArray
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
            const df = verySmallDataFrame;
            expect(df.rows.length).to.equal(3);
            expect(df.columns.length).to.equal(2);
        });

    });

    describe("Row iteration", () => {

        it("Should allow iteration over DataFrame rows in a for...of loop", () => {
            const df = verySmallDataFrame;
            let rowChecker;
            for (const row of df) {
                rowChecker = row;
            }
            // rowChecker should be set to the last row
            expect(rowChecker["id"]).to.equal(verySmallDataFrame.slice(-1).toArray()[0]["id"]);
        });

        it("Should allow destructuring DataFrame rows", () => {
            const df = [...verySmallDataFrame];
            // Compare the
            expect(df.slice(-1)[0]["id"]).to.equal(verySmallDataFrame.slice(-1).toArray()[0]["id"]);
        });

    });

    describe("select()", function() {

        beforeEach(function() {
            this.currentTest.df = verySmallDataFrame;
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
            this.currentTest.df = verySmallDataFrame;
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
            this.currentTest.df = verySmallDataFrame;
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

});
