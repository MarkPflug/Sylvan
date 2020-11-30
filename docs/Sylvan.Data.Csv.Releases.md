# Sylvan.Data.Csv Release Notes
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