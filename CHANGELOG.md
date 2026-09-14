# Changelog

## Unreleased

### Fixed

- Preserve tuple boundaries and value types in `distinct()` instead of dropping unrelated rows.
- Keep non-string values during string replacement and treat selected column names literally.
- Fix window frame boundaries, exact lag/lead offsets, multi-column partitions, and replacement of existing columns. Window evaluation no longer adds internal properties to source rows.
- Evaluate join predicates on actual cell values, reject data-losing column collisions, and preserve unique empty-result schemas and cross-join suffixes.
- Aggregate original rows for pivot counts; support numeric categories and reject output-name collisions.
- Respect absolute output paths, declared column order, and empty export schemas.
- Expose documented `stdev` and `unboundedProceeding` names while retaining `stddev` and `unboundedProceding`.
- Validate invalid window bounds, quantile probabilities, aggregation definitions, pivot columns, and delimiters with descriptive errors.

### Maintenance

- Update dependencies and remove UUID, which was only used for temporary window columns.
- Add regression tests, executable README examples, coverage thresholds, lint/format checks, and isolated npm-package testing.
- Refresh CI for Node 22/24 and Windows; pin Actions and add dependency-update configuration.
- Limit npm package contents, replace scratch examples, and document contributor and release workflows.

### Compatibility

- The supported runtime range is now `^22.13.0 || ^24.0.0`, with CI covering the minimum versions and latest releases in both majors; Node 15/16 are no longer supported.
- Join signatures and the legacy anti-join schema remain unchanged. Predicates run once per actual row pair and never on dummy values. Missing-column validation follows executed branches; overlapping non-key columns and unequal shared values are rejected instead of overwritten.
- Unordered window operations retain input order, including interleaved partitions. Explicit window ordering sorts the output. Invalid frames and missing aggregation columns now throw.
- `distinct()` uses JavaScript Map equality: primitive types stay distinct, NaN compares equal, and objects (including Dates) use reference identity.
- Pivot count includes original rows, even if the value cell is null. Null/undefined pivot categories are omitted. Missing categories return zero for sum/count and null for other aggregations.
- Constructors validate row arrays and require unique string column names, inferring columns when omitted. Empty `fromArray()` input remains invalid; construct an empty DataFrame with explicit columns instead. Row ownership remains shallow and callback purity remains the caller's responsibility.
- TypeScript declarations, a TypeScript rewrite, and performance benchmarks are not part of this update.

## 0.3.0

Historical release. Added pivot support; see Git history for the original changes.
