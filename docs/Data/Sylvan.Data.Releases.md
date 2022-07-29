# Sylvan.Data.Csv Release Notes

_0.2.3_
- Fix an issue binding to `record class` types.
- Add `class` constraint to binder methods, as structs are not currently supported.
- Restore ObjectDataReader methods that were inadvertantly removed in the previous release.

_0.2.2_
- Expose DbDataReaderAdapter base class.
- Add DbDataReader.AsVariableField extension method.
- Add `TakeWhile`.
- Add API documentation.

_0.2.1_
- Adds `DbDataReader.Skip`/`Take` extension methods.
- Adds GetRecordsAsync extension method.
