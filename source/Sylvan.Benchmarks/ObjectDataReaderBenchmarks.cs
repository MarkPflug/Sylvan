using BenchmarkDotNet.Attributes;
using Sylvan.Data;
using System.Collections.Generic;

namespace Sylvan.Benchmarks
{
	[MemoryDiagnoser]
	public class ObjectDataReaderBenchmarks
	{
		ObjectDataReader.Factory<TestRecord> Factory;

		public ObjectDataReaderBenchmarks()
		{
			Factory =
				ObjectDataReader
				.BuildFactory<TestRecord>()
				.AddAllProperties()
				.Build();
		}

		const int Count = 1000000;

		public IEnumerable<TestRecord> GetData()
		{
			return TestData.GetTestObjects(Count, 0);
		}

		[Benchmark(Baseline = true)]
		public void ObjectDataReaderFactory()
		{
			var dr = Factory.Create(GetData());
			dr.Process();
		}

		[Benchmark]
		public void ObjectDataReaderCreate()
		{
			var dr = GetData().AsDataReader();
			dr.Process();
		}

		[Benchmark]
		public void ObjectDataReaderCreateFast()
		{
			var fac =
				ObjectDataReader
				.BuildFactory<TestRecord>()
				.AddColumn("Id", r => r.Id)
				.AddColumn("Date", r => r.Date)
				.AddColumn("IsActive", r => r.IsActive)
				.AddColumn("Name", r => r.Name)
				.Build();
			var dr = fac.Create(GetData());
			dr.Process();
		}

		[Benchmark]
		public void ObjectDataReaderCreateSlow()
		{
			var fac =
				ObjectDataReader
				.BuildFactory<TestRecord>()
				.AddAllProperties()
				.Build();
			var dr = fac.Create(GetData());
			dr.Process();
		}
	}
}
