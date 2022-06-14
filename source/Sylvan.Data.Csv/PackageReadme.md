# Sylvan.Data.Csv

A cross-platform .NET library for reading and writing CSV data files.
The `CsvDataReader` provides readonly, row by row, forward-only access to the data.
Exposes a familiar API via `DbDataReader`, which is ideal for accessing rectangular, tabular data sets.
Sylvan.Data.Csv is currently the [fastest library for reading CSV data](https://github.com/MarkPflug/Benchmarks/blob/main/docs/CsvReaderBenchmarks.md) 
 in the .NET ecosystem.

## Features

- Auto detect delimiters.
- Supports asynchronous IO.
- Strongly-typed accessors that avoid allocations.
	- Supported types includes all standard .NET primitive types,  `DateOnly` and `TimeOnly` on .NET 6, 
	- Binary data encoded with either base64 or hexadecimal.
- Schema information to support database bulk-load operations.

## Usage Examples

### Basic

```C#
using Sylvan.Data.Csv;

// CsvDataReader derives from System.Data.DbDataReader
using CsvDataReader dr = CsvDataReader.Create("data.csv");

// iterate over the rows in the file.
while(dr.Read())
{
	// iterate fields in row.
	for(int i = 0; i < dr.FieldCount; i++)
	{
		var value = dr.GetString(i);
	}
	// Can use other strongly-typed accessors
	// bool flag = edr.GetBoolean(0);
	// DateTime date = edr.GetDateTime(1);
	// decimal amt = edr.GetDecimal(2);
}
```

### Bind CSV data to objects using Sylvan.Data.

```C#
using Sylvan.Data;
using Sylvan.Data.Csv;
using System.Linq;

using var dr = CsvDataReader.Create("data.csv");
IEnumerable<Record> records = dr.GetRecords<Record>();
Record[] allRecords = records.ToArray();

class Record {
	public int Id { get; set; }
	public string Name { get; set; }
	public DateTime Date { get; set; }
	public decimal Amount { get; set; }
}
```

### Convert Excel data to CSV using Sylvan.Data and Sylvan.Data.Excel

```C#
using Sylvan.Data.Csv;
using Sylvan.Data;
using Sylvan.Data.Excel;
using System.Data.Common;

// create reader for excel data file
ExcelDataReader edr = ExcelDataReader.Create("example.xlsx");

// (optional) create data reader which allows variable-length rows
DbDataReader reader = edr.AsVariableField(edr => edr.RowFieldCount);

// create CSV writer to standard out
var csvWriter = CsvDataWriter.Create(Console.Out);

// write excel data as csv
csvWriter.Write(reader);
```