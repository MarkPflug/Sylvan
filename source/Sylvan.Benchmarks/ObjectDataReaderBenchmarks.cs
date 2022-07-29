using BenchmarkDotNet.Attributes;
using Sylvan.Data;
using System.Collections.Generic;

namespace Sylvan.Benchmarks;

[MemoryDiagnoser]
public class ObjectDataReaderBenchmarks
{
	ObjectDataReader.Builder<TestRecord> Builder;

	public ObjectDataReaderBenchmarks()
	{
		Builder =
			ObjectDataReader
			.CreateBuilder<TestRecord>()
			.AddAllProperties();
	}

	const int Count = 1000000;

	public IEnumerable<TestRecord> GetData()
	{
		return TestData.GetTestObjects(Count, 0);
	}

	[Benchmark(Baseline = true)]
	public void ObjectDataReaderFactory()
	{
		var dr = Builder.Build(GetData());
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
			.CreateBuilder<TestRecord>()
			.AddColumn("Id", r => r.Id)
			.AddColumn("Date", r => r.Date)
			.AddColumn("IsActive", r => r.IsActive)
			.AddColumn("Name", r => r.Name);
		var dr = fac.Build(GetData());
		dr.Process();
	}

	[Benchmark]
	public void ObjectDataReaderCreateSlow()
	{
		var fac =
			ObjectDataReader
			.CreateBuilder<TestRecord>()
			.AddAllProperties();
		var dr = fac.Build(GetData());
		dr.Process();
	}
}
