using BenchmarkDotNet.Attributes;

#if NET5_0
using Cesil;
#endif

using Sylvan.Data;
using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;

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
			var rows = csv.GetRecords<CovidRecord>();

			foreach (var row in rows)
			{

			}
		}
#if NET5_0
		[Benchmark]
		public void Cesil()
		{
			var tr = new StringReader(@"Name,Health,Armor,Strength,Agility,Intellect
Gnoll,50,10,10,10,6
Goblin,30,8,7,13,12
Ogre,80,9,15,6,4
");
			var data = CesilUtils.Enumerate<Monster>(tr).ToArray();
			//var o = Options.Default;

			//var c = Configuration.For<CovidRecord>(o);
			//var csv = c.CreateReader(tr);
			//var rows = csv.ReadAll();
		}
#endif
		[Benchmark]
		public void SylvanBench()
		{
			
			var dr = (CsvDataReader)TestData.GetDataWithSchema(o => { o.StringFactory = pool; });

			var binder = DataBinder<CovidRecord>.Create(dr.GetColumnSchema());

			while (dr.Read())
			{
				CovidRecord cr = new CovidRecord();
				binder.Bind(dr, cr);
			}
		}
	}
}
