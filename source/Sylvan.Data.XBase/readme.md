# Sylvan.Data.DBase

The first question is probably: "why"?

The xBase file format (.dbf) is used in [GIS Shape Files](https://en.wikipedia.org/wiki/Shapefile).
There also might be companies still wanting to migrate data out of legacy systems.
My latest run-in with FoxPro was over a decade ago, but I suspect there might still be users out there.
Other that that, there probably isn't a great reason to use this format anymore.

## Features

- Exposes .dbf file through a DbDataReader implementation, so provides a familiar
interface that integrates easily with other.NET data tools.

- Allows reading dBase data files in a forward-only manner, meaning it can stream
data directly out of compressed streams. This is specifically interesting for shapefiles, allowing
reading the data directly from a compressed .zip archive.

- Very high performance and low allocation.

- Uses modern C# language features including async and nullable annotations.

- FoxPro null support.

Supported data types:
- Character
- VarChar
- VarBinary
- Currency
- Numeric/Float
- Double (FoxPro)
- Integer
- Logical (boolean)
- Date
- DateTime
- General
- Blob
- Memo

### Sample data
Example shapefiles that contain dBase .dbf files:
https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html

County 20m, used for testing:
https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_20m.zip

### Format specs
https://www.dbf2002.com/dbf-file-format.html
https://www.clicketyclick.dk/databases/xbase/format/index.html
https://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_NOTE_6_TARGET
https://github.com/yellowfeather/DbfDataReader/issues/35

### Shape files
https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/GovtUnit/Shape/

[US STATES](https://catalog.data.gov/dataset/tiger-line-shapefile-2017-nation-u-s-current-state-and-equivalent-national)
