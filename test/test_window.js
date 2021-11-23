import chai from "chai";
const { expect } = chai;

import {
    smallDataFrame3,

    smallDataFrame3WindowFuncsOrderedPartition
} from "./sample_data.js"

import lorix from "../lorix.js";


describe("Window Functions", () => {

    describe("window()", function() {

        beforeEach(function() {
            this.currentTest.df = smallDataFrame3;

            this.currentTest.windowFuncsOrderedPartition = smallDataFrame3WindowFuncsOrderedPartition;
        });

        it("Should return a new DataFrame with correct window function results as per specified params", function() {
            let winfuncResultNoWinSize = (
                this.test.df
                .withColumn("sum", lorix.window(lorix.sum("salary"),        ["dept"], [["salary"], ["desc"]] ))
                .withColumn("lag", lorix.window(lorix.lag("salary", 1),     ["dept"], [["salary"], ["desc"]] ))
                .withColumn("lead", lorix.window(lorix.lead("salary", 1),   ["dept"], [["salary"], ["desc"]] ))
                .withColumn("stddev", lorix.window(lorix.stddev("salary"),  ["dept"], [["salary"], ["desc"]] ))
                .withColumn("rownum", lorix.window(lorix.rownumber(),       ["dept"], [["salary"], ["desc"]] ))
                .withColumn("stddev", (r) => Math.round(r["stddev"], 0))  // Round to enable comparison for the test
            )

            let winfuncResultWithWinSize = (
                this.test.df
                .withColumn("sum", lorix.window(lorix.sum("salary"),        ["dept"], [["salary"], ["desc"]] ))
                .withColumn("lag", lorix.window(lorix.lag("salary", 1),     ["dept"], [["salary"], ["desc"]] ))
                .withColumn("lead", lorix.window(lorix.lead("salary", 1),   ["dept"], [["salary"], ["desc"]] ))
                .withColumn("stddev", lorix.window(lorix.stddev("salary"),  ["dept"], [["salary"], ["desc"]] ))
                .withColumn("rownum", lorix.window(lorix.rownumber(),       ["dept"], [["salary"], ["desc"]] ))
                .withColumn("stddev", (r) => Math.round(r["stddev"], 0))  // Round to enable comparison for the test
            )

            // Window function without window size defined
            expect(winfuncResultNoWinSize.toArray()).to.deep.equal(this.test.windowFuncsOrderedPartition.toArray());
            expect(winfuncResultNoWinSize.columns).to.deep.equal(this.test.windowFuncsOrderedPartition.columns);

            // Window function with window size defined

        });

        it("Should throw an error when a type other than an array is passed", function() {
            expect(() => {this.test.df.distinct(1)}).to.throw();
            expect(() => {this.test.df.distinct("stringInsteadOfFunction")}).to.throw();
        });

        // it("Should throw an error when referencing a non-existent column", function() {
        //     expect(() => {this.test.df.distinct(["nonExistingColumn"])}).to.throw();
        // });

        // it("Should throw an error if more than one argument is passed", function() {
        //     expect(() => {this.test.df.distinct(["name", "weight"], "anotherArgument")}).to.throw();
        // });

    });

});
