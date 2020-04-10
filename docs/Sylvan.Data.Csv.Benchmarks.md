# Benchmarks

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

|       Method |       Mean | Ratio |    Allocated |
|------------- |-----------:|------:|-------------:|
|    CsvHelper |  23.693 ms |  1.00 |  27257.70 KB |
|  NaiveBroken |   4.673 ms |  0.19 |  11266.88 KB |
|    NLightCsv |  13.910 ms |  0.58 |   7323.29 KB |
|  VisualBasic | 105.455 ms |  4.38 | 187061.38 KB |
|     OleDbCsv | 167.427 ms |  6.96 |   7811.07 KB |
|        NReco |   6.491 ms |  0.27 |   7310.72 KB |
|       Sylvan |   6.033 ms |  0.25 |   7319.43 KB |
|  NRecoSelect |   2.732 ms |  0.11 |    567.14 KB |
| SylvanSelect |   2.796 ms |  0.12 |    372.31 KB |


## CSV Writing

These benchmarks test writing a 100k sequence of object data several typed columns as well as a "grid" of 20 doubles, to a `TextWriter.Null`.

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

### NLightCsv
Sébastian Lorion's [NLight](https://github.com/slorion/nlight) library contains a high-performance CSV parser. Sébastian is also the author of the [Lumenworks CSV](https://www.codeproject.com/Articles/9258/A-Fast-CSV-Reader) parser, which has been around for 15 years now. Lumenworks has been my go-to for CSV parsing for many years.

NLight appears to be as feature rich as Lumenworks, and is also a bit faster.

BenchmarkDotnet, unfortunatley, doesn't seem to cooperate with the Lumenworks library for some reason, so it isn't included in the results, but I've found it ot be faster than CsvHelper, but slower than Nlight.

### VisualBasic
This is `TextFieldParser` included in the Microsoft.VisualBasic library that ships with dotNET. I include this, because it is the only option included as part of the framework libraries.

### OleDbCsv
This benchmark uses the MS Access database driver via OleDb (Windows only, requires a separate install). 
It does do something that no other parser appears to support: it will try to detect the data types of the columns in the CSV file. 
My understanding is that this is done by analyzing the first N rows of the CSV. That comes at the cost of being the slowest CSV parser, by far. 
I've had negative experiences with this feature mis-detecting a column type, when the errant values appear later in a file; the result is usually an exception at runtime.
I suspect the memory metric is misrepresented, because it uses an unmanaged driver so it might not be detectable by the BenchmarkDotNet memory analyzer.

### NReco
Vitaliy Fedorchenko's [NReco.Csv](https://github.com/nreco/csv) was the first CSV library that I found that was close to the performance of Sylvan.Data.Csv. In fact, when I first benchmarked it, it beat Sylvan, which encouraged me to optimize one last hot spot to take the lead. It uses an incredibly similar strategy for parsing to the one I'd used for Sylvan.

### Sylvan
The Sylvan.Data.Csv library, is currently the fastest available CSV parser for dotNET that I'm aware of.

Sylvan offers two CSV writing APIs: `CsvWriter` which offers raw writing capabilities similar to other libraries, and `CsvDataWriter` which writes the a `DbDataReader` data to a `CsvWriter`.

### *Select
The approach that Sylvan and NReco use for processing CSV make them even more efficient when reading only a subset of the columns in a file. These benchmarks measures reading only 3 of the 85 columns.
