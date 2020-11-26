using BenchmarkDotNet.Attributes;
using Cesil;
using Sylvan.Data;
using Sylvan.Data.Csv;
using System.Globalization;

namespace Sylvan.Benchmarks
{
	[MemoryDiagnoser]
	public class CsvDataBinderBenchmarks
	{

		const int BufferSize = 0x4000;
		readonly StringFactory pool;
		char[] buffer = new char[BufferSize];
		readonly StringPool sp;


		public CsvDataBinderBenchmarks()
		{
			this.sp = new StringPool();
			this.pool = new StringFactory(sp.GetString);
		}

		[Benchmark(Baseline = true)]
		public void CsvHelper()
		{
			var tr = TestData.GetTextReader();
			var csv = new CsvHelper.CsvReader(tr, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.CurrentCulture));
			var rows = csv.GetRecords<CovidRow>();

			foreach (var row in rows)
			{

			}
		}

		[Benchmark]
		public void Cesil()
		{
			var tr = TestData.GetTextReader();
			var o = Options.Default;

			var c = Configuration.For<CovidRow>(o);
			var csv = c.CreateReader(tr);
			if (csv.TryRead(out var r))
#pragma warning disable CS0642 // Possible mistaken empty statement
				;
#pragma warning restore CS0642 // Possible mistaken empty statement
			var rows = csv.ReadAll();
		}

		[Benchmark]
		public void SylvanBench()
		{
			var dr = (CsvDataReader)TestData.GetDataWithSchema(o => { o.StringFactory = pool; });

			var binder = DataBinder<CovidRow>.Create(dr.GetColumnSchema());

			while (dr.Read())
			{
				CovidRow cr = new CovidRow();
				binder.Bind(dr, cr);
			}
		}
	}
}
