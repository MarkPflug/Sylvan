# Sylvan.Data.XBase

The fastest XBase data reader available for .NET. XBase is a file format used by DBase, FoxPro, and Clipper database applications which were popular in the '90s. It still used as the storage format for metadata in ESRI shape files.

This library does not support writing XBase files, it only supports reading. 

Sylvan.Data.XBase provides a forward-only reader that allows reading data over a non-seekable stream. This means that it is possible to read records directly out of a zip archive, which is common for shapefiles. Memo data does not neccessarily support forward only reading, but shape files don't normally include memo data.

### Installation

[Sylvan.Data.XBase Nuget Package](https://www.nuget.org/packages/Sylvan.Data.XBase/)

`Install-Package Sylvan.Data.XBase -AllowPrereleaseVersions`

## Example


__Simple Reading__
```C#

using var reader = XBaseDataReader.Create("data.dbf");
while(reader.Read())
{
    var id = reader.GetInt32(0);
    var name = reader.GetString(1);
}

```

__Convert to CSV__
```C#

using var reader = XBaseDataReader.Create("data.dbf");
using var writer = CsvDataWriter.Create("data.csv");
writer.Write(reader);

```