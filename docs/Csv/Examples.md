# API usage by example

It is probably easiest to understand how to use libraries by looking at examples. 
The `CsvDataReader` type implements the `DbDataReader` base type, so using it should be familiar for anyone who
has experience with ADO.NET. This also allows it to interoperate with other ADO.NET providers for things 
like bulk loading data into a database or loading data into a `DataTable`.

## CsvDataReader Examples

### Access columns by ordinal

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
the ordinal should be looked up outside the read loop.

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
similar bulk load cababilities which follow this same pattern.

This example requires the CSV file to contain data that is compatible with
the target table's schema. It applies the schema of the target database table
to the `CsvDataReader` which allows the bulk load operation to
coerce the incomming CSV data to the shape of the table.

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

### Bind to objects with Sylvan.Data

The `Sylvan.Data` package includes a general-purpose data binder, that can bind any properly implemented DbDataReader
to objects.

```C#

class SalesRecord {
    public int Id { get; private set; }
    public string Product { get; private set; }
    public int Quantity { get; private set; }
    public decimal SalePrice { get; private set; }
}

public IEnumerable<SalesRecord> GetRecords(string csvFile) {

    using var csv = CsvDataReader.Create(csvFile);
    var binder = DataBinder.Create<SalesRecord>(csv);

    while(csv.Read()) {
        var record = new SalesRecord();
        binder.Bind(record);
        yield return record;
    }
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
