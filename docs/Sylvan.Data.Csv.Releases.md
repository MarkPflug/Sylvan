# Sylvan.Data.Csv Release Notes
_0.5.0_
Adds support for reading/writeing binary data as base64 encoded strings.

_0.4.5_
Fix bug where schema provided headers aren't used when HasHeaders=false. HasHeaders is only meant to indicate if headers are present in the data file.

_0.4.4_
Fix a bug where ISchemaProvider column name overrides weren't used to populate the headers.
Allows handling CSV with duplicate headers via ISchemaProvider.
Fix bug where headers wouldn't fully initialize when there was a single line, with no newline.

_0.4.3_
Add API documentation.

_0.4.2_
Fix bug with handling improperly quoted fields in CsvDataReader.

_0.4.1_
Fix bug with CsvWriter not handling fields correctly in fast path.

_0.4.0_
Fix bug with CsvDataWriter not handling null columns properly.
Fix bug with CsvWriter spinning when record too large, now throws.
Add CsvWriter options for custom boolean strings.
Fix a performance regression in benchmarks which was caused by external code.

_0.3.4_
Fix a bug with CsvDataReader.GetChars(), which would misbehave when the data reader was used as a table value parameter in a sql query.

_0.3.3_
Default column schema defines a column size (int.MaxValue).

_0.3.2_
GetValue now uses a provided schema to return strongly typed, boxed objects instead of only strings.

_0.3.1_
Remove System.Memory dependency.

_0.3.0_
Adds CsvWriter for, separate from CsvDataWriter.

_0.2.1_ Performance
- Fixes a performance regression introduced with bug fix in 0.2.0.

_0.2.0_ CsvDataWriter
- fixes a bug with handling final newline in CSV

_0.1.2_ Adds column schema to reader.

_0.1.1_ Package metadata change

_0.1.0_ CsvDataReader