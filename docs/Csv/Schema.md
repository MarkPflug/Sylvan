# Schema

The Sylvan CsvDataReader allows providing a schema over the CSV data being read. By default if no custom schema is provided, the data reader will expose  every column as a non-nullable string. This means that `IsDBNull` will always return `false`, and `GetString` will return `""` for all missing and empty fields. However, it is still possible to use all of the primitive accessors (`GetInt32`, `GetFloat`, etc) with the default schema, even though `GetType` will return `typeof(string)`. If a column is known to only contain integer strings, it can be accessed via `GetInt32` even without providing a custom schema.

What does providing a custom schema accomplish? The primary effect is that calls to `GetColumnSchema` and `GetSchemaTable` will return a typed schema. This is important if the `CsvDataReader` is going to be handed to something that knows how to consume such a schema. The two most common such APIS would be either `DataTable.Load(DbDataReader)` or `SqlBulkCopy.WriteToServer`. Both of these APIs will use the provided schema to know how to read the data values in their expected primitive values, rather than their string representations.

Defining a schema is done by implementing the `ICsvSchemaProvider` interface. This interface is quite simple, as it defines a single method with the following signature:
`DbColumn? GetColumn(string? name, int ordinal);`
This method is called for each physical column in the CSV data during initializing. The schema provider can optionally return schema information to the data reader by returning a `System.Data.Common.DbColumn` instance with any number of properties set. Typically, at a minimum, `DataType` and `AllowDBNull` would be defined. If `GetColumn` returns `null`, that column will use the default of non-nullable string.

Sylvan.Data.Csv provides two in-the-box implementations. 

The first is the static `CsvSchema.Nullable` implementation, which will treat all columns as nullable strings. When this schema is used, `IsDBNull` will return `true` for any column that is empty, or whitespace only, or missing. Likewise, `GetValue` will return `DBNull.Value` for such fields.  However, calling `GetString` will still return the string value, not null.

The second is `CsvSchema` type which can be used to apply an existing schema to the data reader. This can be useful when bulk loading data into SqlServer, where the CSV data conforms to the schema of an existing table in the database.

```C#
SqlConnection conn = ...;
var cmd = conn.CreateCommand();

// select an empty record set to get the table schema.
cmd.CommandText = "select top 0 * from [MyTargetTable]";
var reader = cmd.ExecuteReader();
// Get the schema for the SQL table
var tableSchema = reader.GetColumnSchema();
reader.Close();
var options = 
    new CsvDataReaderOptions {
        Schema = new CsvSchema(tableSchema),
    };
```
In this example, the schema of the target table is used to apply the schema to the CSV data that will then be used by `SqlBulkCopy`.

It is also possible to define your own schema provider if needed. The following example defines a simple typed csv schema provider.

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

`ICsvSchemaProvider` can also be used to adjust the names of columns in a CSV. This can be useful for adapting columns to names that are suitable for a data binder. This is done by assigning the `ColumnName` property to a non-empty string. Assigning null, or an empty string will cause the column name in the CSV header to be used.

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

## Format

The CsvDataReader allows defining the format of certain data values via an extension to the DbColumn type. DbColumn does not define a "Format" property explicitly, but it can be extended via the indexer property. If the indexer property returns a string when "Format" is passed, that will be used as the column-specific data type format.

The easiest way to deal with format is to use the `Schema` type defined in the `Sylvan.Data` package. The Schema type offers both a builder API, as well as the ability to de/serialize from a string via `Schema.Parse`. The string representation is as follows:

BaseName>MappedName:Type[Size]?{Format},...

BaseName is used when mapping the value in the CSV header (the base name) to a new name in the CsvDataReader, this is only needed when mapping to a different name. 

MappedName is the name that the header will be mapped to, this is also optional, and the CSV header value will be used when MappedName is empty.

Type is the data type of the column. This can be any of the primitive types in the DbType enum, or "int", "long", or "float".

Size specifies the maximum length of data that is variable length: strings and binary. This is optional, and only useful to define in very narrow scenarios like when a physical database table will be constructed from the provided schema.

"?" indicates that the column is nullable.

{Format} This specifies the data format, which currently only applies to columns of type `DateTime`, `Boolean`, or `Binary`.

### Boolean
The default behavior of GetBoolean is to use Boolean.TryParse which allows only the literal strings "true" and "false", it then falls back to interpreting the field values as a number, in which case it interprets "0" as false, and any other number as true. It is also possible to provide specific true/false strings via `CsvDataReaderOptions`

However, it is also common for CSV to represent boolean values as "Y"/"N" or "T"/"F". These scenarios can be handled by the format string.

The format string for boolean allows one or two specifiers separated by a vertical bar: "
(TrueSpec)?(|FalseSpec)?". If only the TrueSpec is provided that value will represent true and any other value as false. Likewise if only the FalseSpec is provided, e.g. "|Nope", then the string "Nope" will be interpreted as false, and all other strings as true.

### DateTime
The default behavior of GetDateTime is to use DateTime.TryParse to convert the CSV data string using the provided culture, which defaults to `Invariant`. However, it is not unusual for CSV data to contain compact data representations like `yyyyMMdd` which GetDateTime does not handle by default. CsvDataReaderOptions allows specifying a `DateFormat`, which can provide a format that will be used for all column. This can be overridden on a per-column basis by providing a `Format` in the schema spec, e.g. "CreateDate:DateTime{yyyyMMdd}".

### Binary
The default behavior of `GetBytes` is to interpret the field text as base64 encoded binary data. This behavior can be overridden by specifying "hex" as the format. This is most easily done by specifying the option on the `CsvDataReaderOptions.BinaryEncoding` property, but can be overridden on a per-column basis by specifying the "hex" format string, e.g. `"UserId:int,UserProfileImage:binary{hex}"`.