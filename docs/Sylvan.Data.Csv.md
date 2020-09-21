# <img src="../Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Data.Csv

A high-performance, .NET library for reading and writing CSV data files. 
My goal in creating this library was to make the fastest possible CSV parser 
without compromising reliability or usability. While there are a number of 
excellent CSV parsers in the ecosystem, I had an idea for an optimization that 
sent me down the rabbit hole of rolling my own. The result is Sylvan.Data.Csv, which 
I believe is [currently the fastest](Sylvan.Data.Csv.Benchmarks.md), and most memory efficient 
CSV parser available as a Nuget package. I'll be the first to acknowledge that performance 
in CSV parsers isn't the most important feature, correctness of implementation, and ease of use are paramount.
Sylvan CSV has achieved high performance without sacrificing robustness or ease of use.

This library offers a complete CSV implementation: it supports headers, custom delimiters, 
quoted fields, quoted fields containing new lines, escapes, etc. 
The primary class, `CsvDataReader` derives from `System.Data.Common.DbDataReader`, 
and so provides a familiar API for anyone who's worked with ADO.NET. 
Sylvan CSV is also one of the only CSV parsers I'm aware of that supports `async`.

Here is a [brief description](Sylvan.Data.Csv.Design.md) of the strategies used to 
achieve high performance and low memory usage.

#### Installation

[Sylvan.Data.Csv Nuget Package](https://www.nuget.org/packages/Sylvan.Data.Csv/)

`Install-Package Sylvan.Data.Csv`

#### CSV Reader Example

```C#
using var csv = await Sylvan.Data.Csv.CsvDataReader.CreateAsync("demo.csv");

// It is possible to inspect whether any data is available before calling Read for
// the first time. HasRows, Headers and FieldCount are all available at this point.

while(await csv.ReadAsync()) 
{
    var id = csv.GetInt32(0);
    var name = csv.GetString(1);
    var date = csv.GetDateTime(2);
}

```

#### CSV Writer Example

```C#
 // query a table from SqlServer, MySql, etc
DbDataReader dr = await GetDataAsync();

using var csvWriter = new CsvDataWriter("data.csv");
await csvWriter.WriteAsync(dr);

```

## Null/Empty field handling

By default, all fields are considered non-nullable strings. This means that an empty (or missing) field will
be processed as a non-null, empty string. This behavior can be changed by providing `CsvSchema.Nullable`
as the `CsvDataReaderOptions.Schema` property which will treat all columns as nullable strings,
or by providing a custom `ICsvSchemaProvider` implementation which allows customizing the type and nullability
per column.


## Features

_Schema_

`CsvDataReader` supports providing a schema for CSV data. Such a schema is useful if the data is going
to be passed directly to another tool capable of processing the schema, such as `SqlBulkCopy` 
or APIs that bind row data to objects.. By default, columns are treated as non-null strings.
The `Sylvan.Data` package provides APIs for easily defining/serializing schemas, as well as binding row data
to objects.

_Delimiter Detection_

By default, the CsvDataReader will attempt to detect the delimiter being used. 
This is done by counting the number of candidate delimiters in the header row.
Candidate delimiters include: ',', '\t', ';', and '|'. By explicitly specifying
a delimiter, this feature will be disabled.

_Async_

CsvDataReader/Writer both support async operations.

_String Pooling_

CsvDataReader supports de-duplicating strings during reading. CSV files often contain very
reptetive data. By default, repeated values will be returned as a new, duplicated string. However,
it is possible to provide a function which allows de-duplicating strings. The `Sylvan.Data.Csv`
library doesn't include an implementation, but `Sylvan.Common` library provides one via the `StringPool` class.
This implementation is faster and requires fewer allocations than de-duping using a `HashSet\<string\>` after the fact

## Limitations

Record size is the primary limitation. A record must fit entirely within the working buffer.

There is no support for multi-character delimiters. There are plenty of other libraries capable of handling that nonsense.


## Error Handling

_Overlarge Records_

There is one scenario where the CsvDataReader will fail to parse data: when a single record doesn't fit entirely within
the internal buffer. The buffer size can be configured however, so unusually large records can be accomodated
by providing a larger buffer. 

_Missing Fields_

A missing field, meaning a row that contains fewer columns (delimiters) than the header column will be treated
the same as if it were an empty string. Missing fields can be identified by comparing the RowFieldCount to FieldCount.


_Extra Fields_

Extra fields, meaning a row that contains more columns than the header column, will be ignored. 
Extra fields can be identified by comparing the RowFieldCount to FieldCount. 
There is no way to access the parsed extra fields via the `CsvDataReader`.

_Malformed Fields_

A properly constructed and compliant CSV should quote and escape fields containing delimiters or quotes. 
Improperly quoted fields will still be parsed from the file. If a field starts with a quote, it will
be parsed as a quoted field untili the closing quote is found, at which point it will resume un-escaped parsing mode.

Examples:

`a\"\"b` > `a\"\"b` - not considered a quoted string.

`\"a\"b\"` > `ab\"` - considered a malformed, quoted string.
