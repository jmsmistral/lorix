import chai from 'chai';
const { expect } = chai;

import { DataFrame } from '../src/dataframe.js';


describe("DataFrame class", () => {

    describe("DataFrame constructor", () => {

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
            const rows = [
                {'id': 1, 'name': 'billy'},
                {'id': 2, 'name': 'jane'},
                {'id': 3, 'name': 'roger'}
            ];
            const cols = ['id', 'name'];
            const df = new DataFrame(rows, cols);
            expect(df.rows.length).to.equal(3);
            expect(df.columns.length).to.equal(2);
        });

    });

    describe("DataFrame iteration.", () => {

        it("Should allow iteration over DataFrame rows in a for...of loop", () => {
            const rows = [
                {'id': 1, 'name': 'billy'},
                {'id': 2, 'name': 'jane'},
                {'id': 3, 'name': 'roger'}
            ];
            const cols = ['id', 'name'];
            const df = new  DataFrame(rows, cols);
            let rowChecker;
            for (const row of df) {
                rowChecker = row;
            }
            // rowChecker should be set to the last row
            expect(rowChecker["id"]).to.equal(3);
        });

    });

    describe("DataFrame select()", function() {

        beforeEach(function() {
            const rows = [
                {'id': 1, 'name': 'billy'},
                {'id': 2, 'name': 'jane'},
                {'id': 3, 'name': 'roger'}
            ];
            const cols = ['id', 'name'];
            this.currentTest.df = new  DataFrame(rows, cols);
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

    describe("DataFrame drop()", function() {

        beforeEach(function() {
            const rows = [
                {'id': 1, 'name': 'billy'},
                {'id': 2, 'name': 'jane'},
                {'id': 3, 'name': 'roger'}
            ];
            const cols = ['id', 'name'];
            this.currentTest.df = new  DataFrame(rows, cols);
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


});
