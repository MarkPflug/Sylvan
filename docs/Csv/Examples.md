# API usage by example

It is probably easiest to understand how to use libraries by looking at examples. 
Some of the most common use-cases are demonstrated here.

## CsvDataReader Examples

The `CsvDataReader` type implements the `DbDataReader` base type, so using it should be familiar for anyone who
has experience with ADO.NET. This also allows it to interoperate with other ADO.NET providers for things 
like bulk loading data into a database or loading data into a `DataTable`.

### Basic reading with column ordinal

When the order of the columns in the CSV are fixed, it is possible to access them directly by ordinal.

```C#
using Sylvan.Data.Csv;

using var csv = CsvDataReader.Create("demo.csv");

while(await csv.ReadAsync()) 
{
    var id = csv.GetInt32(0);
    var name = csv.GetString(1);
    var date = csv.GetDateTime(2);
}
```

### Access columns by name

When the order of the columns in the CSV are not necessarily fixed, and the column order might change,
the ordinal can be determined from the header names.

```C#
using var csv = CsvDataReader.Create("demo.csv");

var idIndex = csv.GetOrdinal("Id");
var nameIndex = csv.GetOrdinal("Name");
var dateIndex = csv.GetOrdinal("Date");

while(await csv.ReadAsync()) 
{
    var id = csv.GetInt32(idIndex);
    var name = csv.GetString(nameIndex);
    var date = csv.GetDateTime(dateIndex);
}
```

### Bulk Load CSV data into SqlServer

Easily load CSV data into strongly typed data tables.
This example is specific to SqlServer, but most database providers offer
similar bulk load capabilities which follow this same pattern.

This example requires the CSV file to contain data that is compatible with
the target table's schema. It applies the schema of the target database table
to the `CsvDataReader` which allows the bulk load operation to
process CSV data without manually coercing the data types.

```C#

SqlConnection conn = ...;

// Get the schema for the target table
var cmd = conn.CreateCommand();
cmd.CommandText = "select top 0 * from MyDataSet";
var reader = cmd.ExecuteReader();
var tableSchema = reader.GetColumnSchema();

var options = 
    new CsvDataReaderOptions { 
        Schema = new CsvSchema(tableSchema)
    };

using var csv = CsvDataReader.Create("demo.csv", options);

var bcp = new SqlBulkCopy(conn);
bcp.BulkCopyTimeout = 0;
bcp.DestinationTableName = "MyDataSet";
bcp.BatchSize = 10000;
bcp.WriteToServer(dataReader);
```

### CSV Data Binder example

The `Sylvan.Data` library includes a generic data binder which makes it 
very easy to bind to strongly typed objects. This data binder can be used
with any `DbDataReader` instance, and is not specific to `Sylvan.Data.Csv`.

```
using Sylvan.Data;
using Sylvan.Data.Csv;

class Record {
    public int Id {get; set;}
    public int Name {get; set;}
    public int Date {get; set;}
}

using var csv = CsvDataReader.Create("demo.csv");
foreach(var record in csv.GetRecords<Record>())
    var id = record.Id;
    var name = record.Name;
    var date = record.Date;
}
```

### Load strongly-typed data into an ADO.NET data table

The `Schema` type defined in the `Sylvan.Data` package is the easiest way
too apply a typed schema to a CsvDataReader. Applying a strongly-typed schema
allows the ADO.NET code to create a strongly typed DataTable rather than
loading everything as strings.

```C#

var dataTable = new DataTable();

var schema = new CsvSchema(Sylvan.Data.Schema.Parse("Id:int,Name,Quantity:int,SalePrice:decimal"));
using var csv = CsvDataReader.Create("SalesData.csv", new CsvDataReaderOptions { Schema = schema});
dataTable.Load(csv);
```


## CsvDataWriter examples

The `CsvDataWriter` consumes a `DbDataReader` to produce CSV data.

### Data to CSV Writer Example

Any data source that provides a `DbDataReader` can be easily converted to CSV.

```C#
using Sylvan.Data.Csv;
 // query a table from SqlServer, MySql, etc
DbDataReader dr = await GetDataAsync();

using var csvWriter = CsvDataWriter.Create("data.csv");
await csvWriter.WriteAsync(dr);
```

### Object to CSV Writer Example

The `Sylvan.Data` package provides an implementation of `DbDataReader` over
an `IEnumerable<T>`, making it easy to write object data to CSV.

```C#
using Sylvan.Data;
using Sylvan.Data.Csv;

var records = 
	new[]
	{
		new { 
			Id = 1, 
			Name = "Alpha", 
			Date = new DateTime(2021, 1, 1),
		},
		new { 
			Id = 2,
			Name = "Beta", 
			Date = new DateTime(2022, 1, 1),
		},
		new {
			Id = 3,
			Name = "Gamma, Iota, Omega",
			Date = new DateTime(2023, 1, 1),
		}
	};

// create a DbDataReader over the anonymous records.
var recordReader = records.AsDataReader();

using var csvWriter = CsvDataWriter.Create("demo.csv");
await csvWriter.WriteAsync(recordReader);
```
