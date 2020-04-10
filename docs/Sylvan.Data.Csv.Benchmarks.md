# CSV Benchmarks

The benchmark project uses [BenchmarkDotNet](https://github.com/dotnet/BenchmarkDotNet) to compare the performance of some common CSV libraries.
These benchmarks use a large-ish, 3254 rows by 85 columns, CSV file. The API for each library is slightly different, but I think the benchmark setup is fair.

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

|       Method |       Mean | Ratio |   Allocated |
|------------- |-----------:|------:|------------:|
|    CsvHelper |  22.926 ms |  1.00 |  27257.7 KB |
|  NaiveBroken |   4.682 ms |  0.20 |  11266.9 KB |
|    NLightCsv |  13.757 ms |  0.60 |   7323.8 KB |
|  VisualBasic | 106.880 ms |  4.66 | 187061.4 KB |
|     OleDbCsv | 166.456 ms |  7.27 |   7812.2 KB |
|        NReco |   6.834 ms |  0.30 |   7310.7 KB |
|       Sylvan |   5.900 ms |  0.26 |   7302.7 KB |
|  NRecoSelect |   2.831 ms |  0.12 |    567.1 KB |
| SylvanSelect |   2.743 ms |  0.12 |   355.71 KB |


## CSV Writing
|          Method |      Mean | Ratio | Allocated |
|---------------- |----------:|------:|----------:|
| CsvHelperWriter | 21.507 ms |  1.00 |   9.71 MB |
|     NaiveBroken |  6.174 ms |  0.29 |   7.13 MB |
|       NLightCsv | 20.615 ms |  0.96 |   9.69 MB |
|           NReco | 11.702 ms |  0.54 |   7.35 MB |
|          Sylvan |  9.463 ms |  0.44 |   7.41 MB |


### CsvHelper
Josh Close's [CsvHelper](https://github.com/joshclose/CsvHelper) appears to be the go-to CSV parser for dotNET in 2020. It is a full feature library that does a lot more than just parsing CSV. I've used it as the baseline for benchmarks, since it is the most used CSV library on nuget.

### Naive Broken
This measures the naive approach of using `TextReader.ReadLine` and `string.Split` to process CSV data. It is fast, but doesn't handle the edge cases of quoted fields, embedded commas, etc; and so isn't [RFC 4180](https://tools.ietf.org/html/rfc4180) compliant.

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

### *Select
The approach that Sylvan and NReco use for processing CSV make them even more efficient when reading only a subset of the columns in a file. These benchmarks measures reading only 3 of the 85 columns.
