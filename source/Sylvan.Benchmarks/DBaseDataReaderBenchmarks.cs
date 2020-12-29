using BenchmarkDotNet.Attributes;
using Sylvan.Data.XBase;
using System.IO;
using System.IO.Compression;
using System.Net.Http;

namespace Sylvan.Benchmarks
{
	[MemoryDiagnoser]
	public class DBaseDataReaderBenchmarks
	{
		const string DataFileName = "data.zip";
		const string DBaseFileName = "data.dbf";
		const string DataSetUrl = "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_20m.zip";

		readonly byte[] CachedData;

		public DBaseDataReaderBenchmarks()
		{
			if (!File.Exists(DataFileName))
			{
				{
					using var oStream = File.OpenWrite(DataFileName);
					using var iStream = new HttpClient().GetStreamAsync(DataSetUrl).Result;
					iStream.CopyTo(oStream);
				}
				{
					using var stream = File.OpenRead(DataFileName);
					var za = new ZipArchive(stream, ZipArchiveMode.Read);
					var entry = za.GetEntry("cb_2018_us_county_20m.dbf");
					using var oStream = File.Create(DBaseFileName);
					using var iStream = entry.Open();
					iStream.CopyTo(oStream);
				}
			}
			CachedData = File.ReadAllBytes(DBaseFileName);
		}

		[Benchmark]
		public void SylvanDBase()
		{
			var dr = XBaseDataReader.Create(new MemoryStream(CachedData));
			dr.ProcessData();
		}
	}
}
