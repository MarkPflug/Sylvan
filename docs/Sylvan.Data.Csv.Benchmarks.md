# CSV Benchmarks

The benchmark project uses [BenchmarkDotNet](https://github.com/dotnet/BenchmarkDotNet) to compare the performance of some common CSV libraries.
The API for each library is slightly different, but I think the benchmark setup is fair.

These benchmarks were run with the following configuration:
```
BenchmarkDotNet=v0.12.0, OS=Windows 10.0.18363
Intel Core i7-7700K CPU 4.20GHz (Kaby Lake), 1 CPU, 8 logical and 4 physical cores
.NET Core SDK=5.0.100-preview.2.20120.11
  [Host]     : .NET Core 3.1.3 (CoreCLR 4.700.20.11803, CoreFX 4.700.20.12001), X64 RyuJIT
  Job-OFNEKV : .NET Core 3.1.3 (CoreCLR 4.700.20.11803, CoreFX 4.700.20.12001), X64 RyuJIT

Runtime=.NET Core 3.1
```

## CSV Reading

These benchmarks use a large-ish, 3254 rows by 85 columns, CSV file.

|        Method |       Mean | Ratio |    Allocated |
|-------------- |-----------:|------:|-------------:|
|     CsvHelper |  22.640 ms |  1.00 |  27257.75 KB |
|   NaiveBroken |   4.535 ms |  0.20 |  11266.87 KB |
|    FSharpData |  14.490 ms |  0.64 |  62950.06 KB |
|   VisualBasic | 103.258 ms |  4.56 | 187061.14 KB |
|      OleDbCsv | 189.672 ms |  8.38 |   7811.05 KB |
| FastCsvParser |   9.868 ms |  0.44 |   7548.92 KB |
|    CsvBySteve | 170.566 ms |  7.54 |  90645.21 KB |
|  FlatFilesCsv |  32.634 ms |  1.44 |   22545.5 KB |
|         NReco |   6.221 ms |  0.27 |   7214.94 KB |
|        Sylvan |   5.815 ms |  0.26 |    7228.6 KB |
|  SylvanSchema |   6.477 ms |  0.29 |    857.77 KB |
|   NRecoSelect |   2.683 ms |  0.12 |    471.01 KB |
|  SylvanSelect |   2.768 ms |  0.12 |    281.51 KB |


## CSV Writing

These benchmarks write a 100k sequence of object data containing several typed columns as well as a "grid" of 20 doubles, to a `TextWriter.Null`.

|          Method |     Mean | Ratio |    Allocated |
|---------------- |---------:|------:|-------------:|
|   CsvHelperSync | 796.9 ms |  1.00 | 199647.18 KB |
|  CsvHelperAsync |      ??? |     - |            - |
|     NaiveBroken | 402.8 ms |  0.51 | 126982.99 KB |
|       NLightCsv | 577.4 ms |  0.72 | 178710.45 KB |
|           NReco | 528.5 ms |  0.66 | 124637.99 KB |
|     SylvanAsync | 485.1 ms |  0.61 |    506.45 KB |
|      SylvanSync | 423.4 ms |  0.53 |     66.13 KB |
| SylvanDataAsync | 506.5 ms |  0.64 |    518.13 KB |
|  SylvanDataSync | 470.6 ms |  0.59 |     77.77 KB |


### CsvHelper
Josh Close's [CsvHelper](https://github.com/joshclose/CsvHelper) appears to be the go-to CSV parser for dotNET in 2020. It is a full feature library that does a lot more than just parsing CSV. I've used it as the baseline for benchmarks, since it is the most used CSV library on nuget.

The performance of using the CSV writer's async APIs was slow enough that I assume I was using it incorrectly, so I've removed this from the results grid.

### Naive Broken
This measures the naive approach of using `TextReader.ReadLine` and `string.Split` to process CSV data. It is fast, but doesn't handle the edge cases of quoted fields, embedded commas, etc; and so isn't [RFC 4180](https://tools.ietf.org/html/rfc4180) compliant.

Likewise, the writing test is performed by writing commas and newlines, but ignoring escaping.

### FSharp.Data
The FSharp.Data library works perfectly well with C# of course, it also happens to be pretty fast.

### VisualBasic
This is `TextFieldParser` included in the Microsoft.VisualBasic library that ships with dotNET. I include this, because it is the only option included as part of the framework libraries.

### OleDbCsv
This benchmark uses the MS Access database driver via OleDb (Windows only, requires a separate install). 
It does do something that no other parser appears to support: it will try to detect the data types of the columns in the CSV file. 
My understanding is that this is done by analyzing the first N rows of the CSV. That comes at the cost of being one of the slowest CSV parsers tested. 
I've had negative experiences with this feature mis-detecting a column type, when the errant values appear later in a file; the result is usually an exception being thrown.
I suspect the memory metric is misrepresented, because it uses an unmanaged driver so it might not be detectable by the BenchmarkDotNet memory analyzer.

### FastCsvParser
As the name suggests, it is pretty fast.

### CsvBySteve
This is the `Csv` nuget package, by "Steve".

### FlatFilesCsv
The csv parser from the `FlatFiles` nuget package.

### NReco
Vitaliy Fedorchenko's [NReco.Csv](https://github.com/nreco/csv) is an extremely fast CSV parser. 
It uses a very similar technique to Sylvan to attain the performance it does.

### Sylvan
The Sylvan.Data.Csv library, is currently the fastest available CSV parser for dotNET that I'm aware of.

Sylvan offers two CSV writing APIs: `CsvWriter` which offers raw writing capabilities similar to other libraries, and `CsvDataWriter` which writes `DbDataReader` data to a `CsvWriter`.

### SylvanSchema
This measures using the Sylvan CsvDataReader with a provided schema. 
The schema allows parsing primitive values directly out of the text buffer.
This adds a slight amount of time to parse the primitive values, but this is time which would be spent in `Parse` methods anyway if consuming only strings.
This also reduces allocations, since it avoid producing intermediate strings.

### *Select
The approach that Sylvan and NReco use for processing CSV make them even more efficient when reading only a subset of the columns in a file. These benchmarks measures reading only 3 of the 85 columns.
