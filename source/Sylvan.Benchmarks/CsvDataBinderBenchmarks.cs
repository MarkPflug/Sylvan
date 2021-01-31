using BenchmarkDotNet.Attributes;

using Sylvan.Data;
using Sylvan.Data.Csv;

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

		[Benchmark]
		public void SylvanBench()
		{
			
			var dr = (CsvDataReader)TestData.GetDataWithSchema(o => { o.StringFactory = pool; });

			var binder = DataBinder.Create<CovidRecord>(dr);

			while (dr.Read())
			{
				CovidRecord cr = new CovidRecord();
				binder.Bind(dr, cr);
			}
		}
	}
}
