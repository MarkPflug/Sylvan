# <img src="../../Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Data.Csv

Sylvan.Data.Csv is a high-performance .NET library for reading and writing CSV data files.
Of the [many CSV libraries that I've benchmarked](https://github.com/MarkPflug/Benchmarks) it is the
fastest.
It has also been [independently benchmarked](https://www.joelverhagen.com/blog/2020/12/fastest-net-csv-parsers)
among an extensive collection of .NET CSV libraries.

In addition to being fast, it also offers a complete CSV implementation with an intuitive API.
It supports headers, custom delimiters, quoted fields with delimiters and new lines, escapes, etc. 
The primary class, `CsvDataReader`, derives from `System.Data.Common.DbDataReader`, 
and so provides a familiar API for anyone who's worked with ADO.NET, and allows it to
interoperate easily with other common libraries.

#### Installation

[Sylvan.Data.Csv Nuget Package](https://www.nuget.org/packages/Sylvan.Data.Csv/)

`Install-Package Sylvan.Data.Csv`

[Release Notes](Sylvan.Data.Csv.Releases.md)

## [Usage Examples](Examples.md)

A few examples of common use cases are [documented here](Examples.md).

## [Options](Options.md)

The default behavior should be sufficient for most cases, and there are a number of [options](Options.md) for configuring the behavior of the CSV data reader and writer.

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
or APIs that bind row data to objects. By default, columns are treated as non-null strings.
The `Sylvan.Data` package provides APIs for easily defining/serializing schemas, as well as binding row data
to objects.

_Delimiter Detection_

By default, the CsvDataReader will attempt to detect the delimiter being used. 
This is done by counting the number of candidate delimiters in the header row.
Candidate delimiters include: ',', '\t', ';', and '|'. By explicitly specifying
a delimiter, this feature will be disabled.

_Async_

`CsvDataReader` and `CsvDataWriter` both support async operations.

_String Pooling_

CsvDataReader supports de-duplicating strings during reading. CSV files often contain very
repetetive data, so loading each value as a unique string can be extremely wasteful. 
By default, repeated values will be returned as a new, duplicated string. However,
one can optionally provide a function which allows de-duplicating strings. The `Sylvan.Data.Csv`
library doesn't provide an implementation, but `Sylvan.Common` library provides one via the `StringPool` class.
This implementation is faster and requires fewer allocations than de-duping using a `HashSet<string>` after the fact, as
it will de-dupe directly out of the internal CSV read buffer.

```C#
using Sylvan;
using Sylvan.Data.Csv;

var pool = new StringPool();
var opts = new CsvDataReaderOptions { StringFactory = pool.GetString };
var csv1 = CsvDataReader.Create("demo1.csv", opts);
// the pool can be re-used across multiple files
var csv2 = CsvDataReader.Create("demo2.csv", opts);

```

Here are some real-world examples of the impact of string pooling. 
These examples load the entire CSV data set into memory binding to a strongly-typed object.
The memory usage is as reported in Windows Task Manager, so includes more than just the raw data size.

| DataSet | CSV Size | NoPool | NoPoolTime | Pool | PoolTime |
| :- | -: | -: | -: | -: | -: |
|[GNIS feature national file](https://www.usgs.gov/core-science-systems/ngp/board-on-geographic-names/download-gnis-data)|309MB|1215MB|6219ms|685MB|7726ms|
|[FEC individual contributions](https://www.fec.gov/data/browse-data/?tab=bulk-data)|176MB|720MB|2884ms|413MB|3008ms|

In these cases, string pooling significantly reduces the memory required to load these datasets into memory, 
while only requiring a relatively small amount of additional time to load.

String pooling is performed by an external function so that the pool can be shared across multiple datasets. The `StringPool` in Sylvan.Common will only attempt to pool strings that are smaller than 32 characters long by default. As strings get longer it is less likely that they will be duplicated, and also more costly to identify duplicates, so this size limit allows tuning the size vs time.

_Binary data_

CsvDataReader and CsvDataWriter support reading and writing binary data encoded as base64 or hexadecimal, base64 being the default.

```C#
var r = new StringReader("Name,Data\nTest,ABCDEF123456");
var opts = new CsvDataReaderOptions{ BinaryEncoding = BinaryEncoding.Hexadecimal };
var csv = CsvDataReader.Create(r, opts);
csv.Read();
// it is possible to query the required size by passing a null buffer
var len = csv.GetBytes(1, null, 0, 0);
var buffer = new byte[len];
csv.GetBytes(1, buffer, 0, len);
```

_Comments_

While the RFC 4180 makes no mention of comments, it is common that comment lines might appear at the beginning of a CSV starting with the `#` character. Sylvan will skip over such comments lines, and the prefix character can be configured to a different character if needed.

## Limitations

Record size is the primary limitation. A record must fit entirely within the working buffer.

There is no support for multi-character value delimiters. The delimiter must be a non-alphanumeric character in the ASCII character set.

Record delimiters cannot be configured in the `CsvDataReader`, which expects  either `\n` or `\r\n` and allows mixed line endings within the same file. `CsvWriter` can be configured to use either line ending, and defaults to `Environment.NewLine`.

## Error Handling

_Overlarge Records_

CsvDataReader will fail to parse data if a record doesn't fit entirely within the internal buffer. The buffer size can be configured however, so unusually large records can be accomodated by providing a larger buffer. 

_Missing Fields_

A missing field, meaning a row that contains fewer columns than the header column will be treated
the same as if it were an empty string. Missing fields can be identified by comparing the RowFieldCount to FieldCount.

_Extra Fields_

Extra fields, meaning a row that contains more columns than the header column, will be ignored. 
Extra fields can be identified by comparing the RowFieldCount to FieldCount. 
Extra field values can be accessed by ordinal.

_Malformed Fields_

A properly constructed and compliant CSV should quote and escape fields containing delimiters or quotes, either by using RFC4180 standard mode, or by specifying the Unquoted style where characters are escaped instead of being quoted. Improperly quoted fields will result in a FormatException being thrown. Sylvan does not currently provide a mechanism to recover from such formatting errors.
