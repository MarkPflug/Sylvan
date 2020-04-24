# Sylvan.Data.Csv Release Notes

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