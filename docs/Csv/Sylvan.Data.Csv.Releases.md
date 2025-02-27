# Sylvan.Data.Csv Release Notes
_1.4.1_
- Fixes a bug with handling invalid CSV files where the final record contains only a double quote character right before the end of the file.

_1.4.0_
- Improve exception messages when CSV parsing encounters invalid data.
- Adds `CsvInalidCharacterException` which captures more information about parsing failures.
	This derives from `CsvFormatException` so should be compatible with any existing catch blocks.
- CSV parsing exceptions should now report the correct row number. Previously they could be off by one.

_1.3.9_
- Improve the performance of CsvDataReader indexers for some access patterns.

_1.3.8_
- Fixes an issue with detecting MacOS-style ('\r') line ends in some scenarios.
- Field accessors now throw `InvalidOperationException` when called at inappropriate times. 
  This might be a behavior breaking change for code that relied on invalid access patterns.

_1.3.7_
- Adds `CsvDataWriterOptions.QuoteStrings` which now allows quoting empty, non-empty, or all strings. This is useful when string data might otherwise be interpreted as a number. This obsoletes QuoteEmptyString option.

_1.3.6_
- Adds `CsvStyle.Lax` which allows parsing CVS files with invalid fields. In this mode, the parser will not produce exceptions, but will
 do a "best effort" to parse invalid fields.

_1.3.5_
- Fixes a bug where fields could be incorrectly read when the final character was escaped when reading with `CsvStyle.Escaped`.

_1.3.4_
- Adds `CsvSchema.Dynamic` which treats CSV data as having "variant" type. This can be useful when processing
    CSV data where the field type might change from row to row.
- Adds per-column string factory to `CsvDataReader` via `CsvDataReaderOptions.ColumnStringFactory`.

_1.3.3_
- CsvDataReader.Create now accepts a CancellationToken to support async cancellation during construction.

_1.3.2_
- Improve SIMD code path for significant performance gains. Thanks to [nietras](https://github.com/nietras/sep)
  and their impressive sep library for inspiration and learning.

_1.3.1_
- Add support for reading CSV files that use '\r' as line terminators.
- CsvDataWriter now defaults to writing '\n', regardless of platform.

_1.3.0_
- Drop support for netcoreapp3.0.
- Fixes CsvDataReader reading of empty enum fields.
- Add CsvDataReader support for reading "dynamic" object-typed data, where each row might contain
    different typed values.

_1.2.7_
- Fixes a bug in CsvDataWriter when writing `long` values from an untyped data source.

_1.2.6_
- CsvDataWriter handles `DbDataReader` implementations that don't fully expose schema information.
- CsvDataWriter uses appropriate formatting when processing data that is not uniformly typed.

_1.2.5_

- CsvDataReader now identifies empty quoted fields (`""`) as null when a 
   strongly-typed schema indicates the column is a nullable non-string type.
- Adds CsvDataReader.Initialize method which allows re-initializing the schema.
	Supports handling files that have extra rows at the start of the file.

_1.2.4_

- CsvDataWriter avoids array allocation for default configuration.
- Adds missing `ConfigureAwait` calls.
- Fixes some issues where invalid CSV data could be written in some cultures.

_1.2.3_

- Fixes accessing enum values via the `GetValue` accessor. Thanks to [0xced](https://github.com/0xced).

_1.2.2_

- CsvSchema allows renaming columns when BaseColumnName is set.

_1.2.1_

- Adds MaxBufferSize option to CsvDataWriter, which allows internal buffer to grow when necessary.
- Change buffering behavior of CsvDataWriter to have symmetry with CsvDataReader. It was previously possible to write CSV files that could not be read back with the same configuration. This was because the internal buffer for the writer only required that the current field fit in the buffer, while the reader requires the entire record (all fields). The CsvDataWriter now buffers an entire record instead.
- Allow accessing extra columns via CsvDataReader.GetValue().
- Fix DateOnly and TimeOnly custom formatting in CsvDataWriter.
- CsvDataReader.GetValue will return DateOnly/TimeOnly when specified by schema.
- CsvDataWriterOptions.DateFormat is marked obsolete, replace with DateTimeFormat.
- Add CsvDataWriter.QuoteEmptyStrings to indicate that empty strings should be written as empty quotes instead of an empty field.

_1.2.0_

- Adds MaxBufferSize option to CsvDataReader, which allows the internal buffer to grow when necessary. 
  Defaults to `null`, which disallows growth, and is consistent with the previous behavior.
- Adds `ICsvSchemaProvider.GetFieldCount()` which gives the provider the ability to explicitly
  control the number of columns. 
  Previously, the field count of the current row would be used to determine the number of
  columns. This is an (unlikely) breaking change for users on .NET framework, who will need
  to provide an implementation.
- Add CsvSchemaProvider base implementation of ICsvSchemaProvider. Implementers should prefer the abstract base to avoid potential future breaking changes.
- Add `CsvDataReaderOptions.DateTimeFormat` to replace `DateFormat` which is now `Obsolete`.
- Add `CsvDataReaderOptions.DateOnlyFormat` to allow globally specifying the format for parsing DateOnly values.
- Improve the performance of CsvDataReader async operations in some scenarios.

_1.1.16_

- CsvDataWriter support for writing variable field count.
- CsvDataReader no longer skips empty leading rows in SingleResult mode.

_1.1.15_

- Fix for CSV comment not being terminated, and being included in subsequent row.

_1.1.14_

- Fix for reading CSV file containing a comment at the end of the file with no line terminator.

_1.1.13_

- Expose `CsvDataReader.GetFieldSpan`.

_1.1.12_

- Fix an issue where CsvRecordTooLargeException would be thrown for a single-line file.

_1.1.11_

- Fix an issue where the final field in a file would be read incorrectly if it was both quoted and there was no trailing newline.

_1.1.10_

- Fix behavior when handling empty files and HasHeader option is false.

_1.1.9_

- Add support for writing Enum values via CsvDataWriter.
- CsvDataReaderOptions and CsvDataWriterOptions `Buffer` property moved to be a separate parameter to constructors. 
Existing options property continues to work, but marked `Obsolete`. 
This was done to allow options objects to be reused, but the buffer can't be shared between instances.
- CsvDataWriterOptions allows configuring binary format.

_1.1.8_

- Support custom formatting for CsvDataReader DateTimeOffset, DateOnly, TimeOnly, and TimeSpan.
- Fixes #79: `MultiResult` mode now correctly handles cases where a header row can exist without following data rows.
- Improves date parsing performance for ISO formatted dates.

_1.1.7_

- Fix for SIMD logic when encountering a quote that would cause incorrect number of records to be identified.
- Fix issue with final character in file being a quote.
- Increase the default buffer size for CsvDataReader and CsvDataWriter from 4k to 16k to be able handle
more extreme scenarios without requiring configuration.

_1.1.6_

- Improve performance of CsvDataReader SIMD logic on AMD Zen2 architecture.

_1.1.5_

- Fix regression introduced in 1.1.4.

_1.1.4_

- Fix for issue with detecting overlarge records when reading asynchronously, or creating a new reader.

_1.1.3_

- Improve CsvDataReader performance for some files that contain a lot of quoted fields.

_1.1.2_

- Add CsvDataReader support for accessing enum values via `GetFieldValue<T>`.
- Fix for issue #67, which was caused by some incorrect simd logic.

_1.1.1

- Fixes some capabilities exposed in netcoreapp3.0.

_1.1.0_

- CsvDataReader now provides a proper implementation overriding `DbDataReader.GetFieldValue<T>`.
- Adds a `net6.0` target framework.
- Adds CsvDataReader support for reading GetDate and GetTime for `net6.0`.
- Adds CsvDataReader support for reading `DateTimeOffset` and `TimeSpan` to all supported frameworks.
- Adds support for async cancellation.
- Improve CsvDataReader performance in some scenarios.

_1.0.3_

- Fix CsvDataWriter not writing certain types when writing the value would span a buffer.

_1.0.2_

- Fix CsvDataWriter.WriteAsync to consume the data reader asynchronously with `ReadAsync` instead of `Read`, 
which caused failures when used in ASP.NET Core where synchronous IO is disallowed by default.
- Seal CsvDataWriter class.
- Fix bug that could allow reading invalid hex binary data, will now throw an exception instead of producing invalid result.

_1.0.1_

- Adds support for Unquoted style to CsvDataWriter.
- Fix for sync IO when performing async read on CsvDataReader.

_1.0.0_

- Adds ability to read comments via CommentHandler in CsvDataReader.
- Writes date values (where time component is at midnight) with `DateFormat` as opposed to `DateTimeFormat`.

_0.10.1_

- Fix potential issue with reading a comment line where the newline sequence spans a buffer boundary.
- Add source link.

_0.10.0_

- Adds support for Unquoted CSV parsing style where fields with delimiters and newlines don't use quotes, but only use escaping.
- Removed CsvWriter. CsvDataWriter will continue to exist.
- CsvWriterOptions renamed to CsvDataWriterOptions.
- Add WriteHeader option to CsvDataWriterOptions.
- Performance improvements for real-world usage scenarios. Micro-benchmark measurements remain stable.
- CsvDataWriter constructor removed and replaced with static Create function.
- Drop support for "broken quotes" mode. All reading is now strict.
- Multiple result sets support.

_0.9.2_

- Add support for per-column boolean format specification.
- Add CsvDataReader.GetRawRecord and GetRawRecordSpan methods.
- Configure binary encoding for whole file via options.
- Add support for tolerating "0x" prefix on hex encoded values.

_0.9.1_

- Add support for hexadecimal encoded binary data.
- Performance improvement for synchronous accessors.

_0.9.0_

- Collapse AutoDetect (`bool`) and Delimiter (`char`) options into Delimiter (`char?`).
- Minor numeric parsing performance improvement.
- Allow loading duplicate headers, but disallow access to those columns by name.

_0.8.3_

- Csv nullable columns treat whitespace-only as null.

_0.8.2_

- Minor `CsvDataReader` performance improvement.

_0.8.1_

- Avoid a potential buffer copy when processing binary (Base64) columns.
- GetByte and GetChar return the full length of data when passed a null buffer. This matches the same behavior of most other DbDataReader implementations.

_0.8.0_

- Add some nullability annotations to align with new annotations exposed in net5.0.
	Most notably, this means that schema column names can no longer be reported as null, 
	but will instead be empty string when no headers are present.

_0.7.4_

- Remove usage of ValueTuple, which is apparently problematic in some scenarios.

_0.7.3_

- Support user-allocated buffer, to allow pooling.
- More consistent default-schema behavior around null/empty. (Technically breaking, but shouldn't affect reasonable code.)
- Nullability annotations aligned with BCL base types.
- Expose RowFieldCount to allow detecting missing/extra fields in rows.

_0.7.2_

- Fix a bug when there is an incomplete record, final comma, as the last character in the file.

_0.7.1_

- Allow integer columns to be read as boolean.

_0.7.0_

- CsvDataWriter returns number of rows written. I think this is a breaking change for anyone using CsvDataWriter.
- Support for column series in CsvSchema.

_0.6.4_

- Add support for optional string de-duping.

_0.6.3_

- Add AutoDetect delimiter feature.

_0.6.2_

- Fix a bug introduced in 0.6.1 refactoring.
- Add DateFormat support.

_0.6.1_

- Minor csv data reader perf improvement.

_0.6.0_

- Add CsvSchema implementation.
- Add CsvDataReader/CsvWriter constructor convenience overloads for filenames.
- Add ability to specify true/false strings for CsvDataReader boolean values.

_0.5.0_

- Adds support for reading/writing binary data as base64 encoded strings.

_0.4.5_

- Fix bug where schema provided headers aren't used when HasHeaders=false. HasHeaders is only meant to indicate if headers are present in the data file.

_0.4.4_

- Fix a bug where ISchemaProvider column name overrides weren't used to populate the headers.
- Allows handling CSV with duplicate headers via ISchemaProvider.
- Fix bug where headers wouldn't fully initialize when there was a single line, with no newline.

_0.4.3_

- Add API documentation.

_0.4.2_

- Fix bug with handling improperly quoted fields in CsvDataReader.

_0.4.1_

- Fix bug with CsvWriter not handling fields correctly in fast path.

_0.4.0_

- Fix bug with CsvDataWriter not handling null columns properly.
- Fix bug with CsvWriter spinning when record too large, now throws.
- Add CsvWriter options for custom boolean strings.
- Fix a performance regression in benchmarks which was caused by external code.

_0.3.4_

- Fix a bug with CsvDataReader.GetChars(), which would misbehave when the data reader was used as a table value parameter in a sql query.

_0.3.3_

- Default column schema defines a column size (int.MaxValue).

_0.3.2_

- GetValue now uses a provided schema to return strongly typed, boxed objects instead of only strings.

_0.3.1_

- Remove System.Memory dependency.

_0.3.0_

- Adds CsvWriter, separate from CsvDataWriter.

_0.2.1_ Performance

- Fixes a performance regression introduced with bug fix in 0.2.0.

_0.2.0_ 

- CsvDataWriter
- fixes a bug with handling final newline in CSV

_0.1.2_ 

- Adds column schema to reader.

_0.1.1_ 

- Package metadata change

_0.1.0_ 

- CsvDataReader
