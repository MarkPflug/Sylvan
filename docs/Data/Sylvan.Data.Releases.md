# Sylvan.Data.Csv Release Notes

_0.2.6_
- Adds support for reading `decimal` properties with ObjectDataReader.
- Adds support for binding DateOnly and TimeOnly values.

_0.2.5_
- Expose GetRecordsAsync to net6.0.

_0.2.4_
- Add experimental DbDataReader.ValidateSchema.
- Add support for binding to DateOnly and TimeOnly.

_0.2.3_
- Fix an issue binding to `record class` types.
- Add `class` constraint to binder methods, as structs are not currently supported.
- Restore ObjectDataReader methods that were inadvertently removed in the previous release.

_0.2.2_
- Expose DbDataReaderAdapter base class.
- Add DbDataReader.AsVariableField extension method.
- Add `TakeWhile`.
- Add API documentation.

_0.2.1_
- Adds `DbDataReader.Skip`/`Take` extension methods.
- Adds GetRecordsAsync extension method.
