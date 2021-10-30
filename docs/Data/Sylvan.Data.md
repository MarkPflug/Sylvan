# <img src="../../Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Data

The `Sylvan.Data` library provides types for working with ADO.NET data objects, primarily the `DbDataReader` type.
Many of these were developed to be used with the `Sylvan.Data.Csv` package, but they aren't specific to CSV and 
can be used with any compatible provider.

## Data Binder

This library implements a high-performance, general-purpose data binder.
The data binder was originally implemented for use with Sylvan.Data.Csv,
but can be used with any `DbDataReader` implementation.

```C#
using Sylvan.Data;
using Sylvan.Data.Csv;

var csv = CsvDataReader.Create("demo.csv");
var binder = DataBinder.Create<Record>(csv);

while (csv.Read())
{
	var item = new Record();
	binder.Bind(csv, item);
}

class Record
{
	public int Id { get; set; }
	public string Name { get; set; }
	public DateTime Date { get; set; }
}

```

The data binder doesn't allocate the instance which to bind to.
This design allows a single instance to be reused, 
which can be useful in data streaming scenarios where only instance will be processed at a time.

For convenience, there is an extension method that makes the data binder easier to use.
This extension requires the type have a public default constructor, as it allocates a new record per row.

```C#
var csv = CsvDataReader.Create("demo.csv");
var records = csv.GetRecords<Record>();

foreach(var record in records) 
{
    // process record
}
```

## Object Data Reader

The inverse operation, converting `IEnumerable<T>` to `DbDataReader`, is also supported by the `ObjectDataReader` type.
The `Create` method will create a `DbDataReader` exposing all public properties as columns.

```C#
using Sylvan.Data;
using Sylvan.Data.Csv;

IEnumerable<Record> records = ...;
DbDataReader reader = ObjectDataReader.Create(records);
// or via extension method
// reader = records.AsDataReader();

using var csvWriter = CsvDataWriter.Create("demo.csv");
csvWriter.Write(reader);
```

The `ObjectDataReader` can be customized by using the `Build` method, which allows explicitly specifying the columns.

```C#
using Sylvan.Data;

IEnumerable<Record> records = ...;

var builder =
    ObjectDataReader
    .Build<Record>()
    .AddColumn("Id", r => r.Id)
    .AddColumn("Date", r => r.Date);

DbDataReader reader = builder.Build(records);
```


## Extension

There are some helpful extension methods included as well that provide simple LINQ-like
operations on `DbDataReader`. 
Since `DbDataReader` only supports one-shot, forward-only processing of data,
there are limitations to how these methods can be used.

### Select

The `Select` method allows selecting a subset of the columns in the data reader.
The selected columns can be specified by name or by ordinal.

```C#
var dataRader = CsvDataReader.Create("demo.csv");
// create a data reader that only exposes two columns
dataReader = dataReader.Select("Id", "Name");
```

### Where

The `Where` method allows selecting a subset of the rows in a data reader.

```C#
var dataRader = CsvDataReader.Create("demo.csv");
// create a data reader that only exposes two columns
var dateOrdinal = dataReader.GetOrdinal("Date");
var limit = new DateTime(2020, 1, 1);
dataReader = dataReader.Where(dr => dr.GetDateTime(dateOrdinal) < limit);
```