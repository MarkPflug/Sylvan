# Sylvan.Data

A .NET library for working with data. Provides implementations of, and extensions for, `DbDataReader`. 
All of the data operations are designed to support streaming, so large data sets don't need to fit in memory.

## DataBinder

Contains a general purpose data binder that can bind `DbDataReader` records to object instances. 
The following example binds CSV data using Sylvan.Data.Csv library to strongly-typed C# `Record` instances.

```
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

## Object data reader

The `IEnumerable<T>.AsDataReader` extension method provides the inverse of the databinder, 
exposing object instances as a `DbDataReader`.

This example demonstrates using the Sylvan.Data.Csv library to write object data.

```
IEnumerable<Record> records = GetRecords();
DbDataReader reader = records.AsDataReader();
CsvDataWriter csvWriter = CsvDataWriter.Create("records.csv");
csvWriter.Write(reader);
```

## Extension methods

The library includes various LINQ-like extension methods for `DbDataReader`.

_Select_

Creates a DbDataReader that exposes a subset of the columns.

_Where_

Creates a DbDataReader that filters rows.

_Skip/Take_

Creates a DbDataReader that skips rows, or limits the rows read.

_WithColumns_

Creates a DbDataReader with additional columns attached.


This example shows how to use some of these methods in a CSV data loading operation.

```C#
using Sylvan.Data;

string csvFile = "data.csv";
var importDate = DateTime.UtcNow;
var csv = CsvDataReader.Create(csvFile);

DbDataReader dr =
	csv
	// select some of the columns from the csv file
	.Select("Id", "Name", "Date", "Value")
	// filter to just records from the last week
	.Where(d => d.GetDateTime(2) >= importDate.AddDays(-7))
	// add columns with information about the data source
	.WithColumns(
		new CustomDataColumn<string>("SourceFile", r => csvFile),
		new CustomDataColumn<DateTime>("ImportDate", r => importDate),
		new CustomDataColumn<int>("RowNum", r => csv.RowNumber)
	)
	// limit to 10k records
	.Take(10000);

LoadData(dr);
```

## Schema

The `Schema` type provides methods to building and de/serializing data schema information. 
This can be used to attach schema information to weakly typed data files, like CSV and Excel data, 
using the Sylvan.Data.Csv and Sylvan.Data.Excel package.
