# Sylvan.Data.Csv Release Notes

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
