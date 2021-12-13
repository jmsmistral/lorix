import chai from "chai";
const { expect } = chai;

import {
    smallDataFrame3,

    smallDataFrame3WindowFuncsOrderedPartition,
    smallDataFrame3WindowFuncsWindowSize
} from "./sample_data.js"

import lorix from "../lorix.js";


describe("Window Functions", () => {

    describe("window()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame3;

            this.currentTest.windowFuncsOrderedPartition = smallDataFrame3WindowFuncsOrderedPartition;
            this.currentTest.windowFuncsWindowSize = smallDataFrame3WindowFuncsWindowSize;
        });

        it("Should return a new DataFrame with correct window function results as per specified params", function() {
            let winfuncResultNoWinSize = (
                this.test.df
                .withColumn("sum", lorix.window(lorix.sum("salary"),        ["dept"], [["salary"], ["desc"]] ))
                .withColumn("lag", lorix.window(lorix.lag("salary", 1),     ["dept"], [["salary"], ["desc"]] ))
                .withColumn("lead", lorix.window(lorix.lead("salary", 1),   ["dept"], [["salary"], ["desc"]] ))
                .withColumn("stddev", lorix.window(lorix.stddev("salary"),  ["dept"], [ ["salary"], ["desc"]] ))
                .withColumn("rownum", lorix.window(lorix.rownumber(),       ["dept"], [["salary"], ["desc"]] ))
                .withColumn("stddev", (r) => Math.round(r["stddev"], 0))  // Round to enable comparison for the test
            )

            let winfuncResultWithWinSize = (
                this.test.df
                .withColumn("sum", lorix.window(lorix.sum("salary"),          ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("lag", lorix.window(lorix.lag("salary", 1),       ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("lead", lorix.window(lorix.lead("salary", 1),     ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("stddev", lorix.window(lorix.stddev("salary"),    ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("variance", lorix.window(lorix.variance("salary"),["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("mean", lorix.window(lorix.mean("salary"),        ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("median", lorix.window(lorix.median("salary"),    ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("min", lorix.window(lorix.min("salary"),          ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("max", lorix.window(lorix.max("salary"),          ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("rownum", lorix.window(lorix.rownumber(),         ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow] ))
                .withColumn("stddev", (r) => r["stddev"] !== null ? Math.round(r["stddev"], 0) : null)  // Round to enable comparison for the test
                .withColumn("variance", (r) => r["variance"] !== null ? Math.round(r["variance"], 0) : null)
                .withColumn("mean", (r) => r["mean"] !== null ? Math.round(r["mean"], 0) : null)
                .withColumn("median", (r) => r["median"] !== null ? Math.round(r["median"], 0) : null)
            )

            // Window function without window size defined
            expect(winfuncResultNoWinSize.toArray()).to.deep.equal(this.test.windowFuncsOrderedPartition.toArray());
            expect(winfuncResultNoWinSize.columns).to.deep.equal(this.test.windowFuncsOrderedPartition.columns);

            // Window function with window size defined
            expect(winfuncResultWithWinSize.toArray()).to.deep.equal(this.test.windowFuncsWindowSize.toArray());
            expect(winfuncResultWithWinSize.columns).to.deep.equal(this.test.windowFuncsWindowSize.columns);
        });

        it("Should throw an error when invalid window parameters passed", function() {
            expect(() => {this.test.df.withColumn("sum", lorix.window(1, ["dept"], [["salary"], ["desc"]], [1, lorix.currentRow]))}).to.throw();
            expect(() => {this.test.df.withColumn("sum", lorix.window(lorix.sum("salary"), 1, [["salary"], ["desc"]], [1, lorix.currentRow]))}).to.throw();
            expect(() => {this.test.df.withColumn("sum", lorix.window(lorix.sum("salary"), ["dept"], 1, [1, lorix.currentRow]))}).to.throw();
            expect(() => {this.test.df.withColumn("sum", lorix.window(lorix.sum("salary"), ["dept"], [["salary"], ["desc"]], 1))}).to.throw();
        });

        it("Should throw an error when referencing a non-existent column", function() {
            expect(() => {this.test.df.withColumn("sum", lorix.window(lorix.sum("salary"), ["nonExistentColumn"], [["salary"], ["desc"]], [1, lorix.currentRow]))}).to.throw();
            expect(() => {this.test.df.withColumn("sum", lorix.window(lorix.sum("salary"), ["dept"], [["nonExistentColumn"], ["desc"]], [1, lorix.currentRow]))}).to.throw();
        });

        // Standard window function tests
        describe("rownumber()", function() {

            it("Should throw an error when a parameter is passed", function() {
                expect(() => {this.test.df.withColumn("sum", lorix.window(lorix.rownumber("salary")))}).to.throw();
            });

        });

    });

});
