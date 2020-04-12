# <img src="../Sylvan.png" height="48" alt="Sylvan Logo"/> Sylvan.Data.Csv

A high-performance, .NET library for reading CSV data files. My goal in creating this library was to make the fastest possible CSV parser 
without compromising reliability or usability. While there are a number of excellent CSV parsers in the ecosystem, I had an idea
for a potential optimization that sent me down the rabbit hole of rolling my own. The result is Sylvan.Data.Csv, which I believe is [currently
the fastest](Sylvan.Data.Csv.Benchmarks.md), and most memory efficient CSV parser available as a Nuget package. I'll be the first to acknowledge that performance 
in CSV parsers isn't the most important feature, correctness of implementation, and ease of use are paramount.
Sylvan CSV has achieved high performance without sacrificing robustness or ease of use.

This library offers a complete CSV implementation: it supports headers, custom delimiters, quoted fields, quoted fields containing new lines, escapes, etc. 
Other libraries may offer more in the way of data binding, malformed field handling, and error recovery, so they may be better options
for some applications. The primary class, `CsvDataReader` derives from `System.Data.Common.DbDataReader`, so it should be familiar API 
for anyone who's worked with ADO.NET. Sylvan CSV is also one of the only CSV parsers I'm aware of that supports `async`.

Here is a [brief description](Sylvan.Data.Csv.Design.md) of the strategies used to achieve high performance and low memory usage.

#### Installation

[Sylvan.Data.Csv Nuget Package](https://www.nuget.org/packages/Sylvan.Data.Csv/)

`Install-Package Sylvan.Data.Csv`

#### CSV Reader Example

```C#
using var tr = File.OpenText("demo.csv");
var csv = await Sylvan.Data.Csv.CsvReader.CreateAsync(tr);

// It is possible to inspect whether any data is available before calling Read for
// the first time. HasRows, Headers and FieldCount are all available at this point.

while(await csv.ReadAsync()) 
{
    var id = csv.GetInt32("Id");
    var name = csv.GetString("Name");
    var date = csv.GetDateTime("Date");
}

```

#### CSV Writer Example

```C#
 // query a table from SqlServer, MySql, etc
DbDataReader dr = await GetDataAsync();

using var tw = File.CreateText("data.csv");
var csvWriter = new CsvDataWriter(tw);
await csvWriter.WriteAsync(dr);

```
