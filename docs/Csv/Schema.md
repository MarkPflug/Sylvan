# Schema

The Sylvan CsvDataReader allows providing a schmea over the CSV data being read. By default, if no custom schema is provided, the data reader will expose  every column is a non-nullable string. This means that `IsDBNull` will always return `false`, and `GetString` will return `""` (empty string) for all missing and empty fields. However, it is still possible to use all of the primitive accessors (`GetInt32`, `GetFloat`, etc) with the default schema, even though `GetType` will return `typeof(string)`. So, if a column is known to only contain integer strings, it can be accessed via `GetInt32` even without providing a custom schema.

So, what does providing a custom schema accomplish? The primary effect is that calls to `GetColumnSchema` and `GetSchemaTable` will return a typed schema. This is important if the `CsvDataReader` is going to be handed to something that knows how to consume such a schema. The two most common such APIS would be either `DataTable.Load(DbDataReader)` or `SqlBulkCopy.WriteToServer`. Both of these APIs will use the provided schema to know how to read the data values in their expected primitive values, rather than their string representations. There are also general purpose databinding APIs like Dapper's `GetRowParser<T>` that can abstractly bind a `DbDataReader` to a strongly typed object. The Sylvan.Data library (still prerelease) also implements a similar general purpose binder in `Sylvan.Data.DataBinder`. The API for this is still in flux, and I don't know when I'll get around to finalizing it, so I wouldn't recommend using it yet.

Defining a schema is done by implementing the `ICsvSchemaProvider` interface. This interface is quite simple, as it defines a single method with the following signature:
`DbColumn? GetColumn(string? name, int ordinal);`
This method is called for each physical column in the CSV data, when the `CsvDataReader` is initializing. The schema provider can optionally return schema information to the data reader by returning a `DbColumn` instance with any number of properties set. Typically, at a minimum `DataType` and `AllowDBNull` would be defined. If `GetColumn` returns `null`, that column will use the default of non-nullable string.


Sylvan.Data.Csv provides two in-the-box implementations. 

The first is the static `CsvSchema.Nullable` implementation, which will treat all columns as nullable strings. When this shema is used, `IsDBNull` will return `true` for any column that is empty, or whitespace only, or missing. However, calling `GetString` will still return the string value, not null.

The second is `CsvSchema` type itself which can be used to apply an existing schema to the data reader. This can be useful when bulk loading data into SqlServer.

```C#
SqlConnection conn = ...;
var cmd = conn.CreateCommand();

// select an empty record set to get the table schema.
cmd.CommandText = "select top 0 * from [MyTargetTable]";
var reader = cmd.ExecuteReader();
var tableSchema = reader.GetColumnSchema();
reader.Close();
var options = 
    new CsvDataReaderOptions {
        Schema = new CsvSchema(tableSchema),
    };
```
In this example, the schema of the target table is used to apply the schema to the CSV data that will then be used by `SqlBulkCopy`.

It is also possible to define your own schema provider if needed.

```C#
class TypedCsvColumn : DbColumn
{
    public TypedCsvColumn(Type type, bool allowNull)
    {
        this.DataType = type;
        this.AllowDBNull = allowNull;
    }
}
    
class TypedCsvSchema : ICsvSchemaProvider
{
    List<TypedCsvColumn> columns;

    public TypedCsvSchema()
    {
        this.columns = new List<TypedCsvColumn>();
    }

    public TypedCsvSchema Add(Type type, bool isNullable)
    {
        this.columns.Add(new TypedCsvColumn(type, allowNull));
        return this;
    }

    public TypedCsvSchema Add<T>()
    {
        var type = typeof(T);
        bool allowNull = false;
        var baseType = Nullabe.GetUnderlyingType(type);
        if(baseType != null) {
            type = baseType;
            allowNull = true;
        }

        return Add(type, all)
    }

    DbColumn? ICsvSchemaProvider.GetColumn(string? name, int ordinal)
    {
        return ordinal < columns.Count ? columns[ordinal] : null;
    }
}
```

This schema could then be used to define a schema for a CSV with 4 columns, as follows:

```C#

var schema = 
    new TypedCsvSchema()
    .Add<string>()
    .Add<DateTime>()
    .Add<int>()
    .Add<float?>();

```

`ICsvSchemaProvider` can also be used to adjust the names of columns in a CSV. This can be useful for adapting columns to names that are suitable for a databinder. This is done by assigning the `ColumnName` property to a non-empty string. Assigning null, or an empty string will cause the column name in the CSV header to be used.

As an example, the following provider would assign Excel-style names to each column:

```C#
class ExcelHeaders : ICsvSchemaProvider
{
	public DbColumn GetColumn(string name, int ordinal)
	{
		return new ExcelColumn("" + (char)('A' + ordinal));
	}

	class ExcelColumn : DbColumn
	{
		public ExcelColumn(string name)
		{
			this.ColumnName = name;
		}
	}
}

```