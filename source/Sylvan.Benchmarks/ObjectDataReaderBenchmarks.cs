using BenchmarkDotNet.Attributes;
using Sylvan.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit.Sdk;

namespace Sylvan.Benchmarks
{
	[MemoryDiagnoser]
	public class ObjectDataReaderBenchmarks
	{

		ObjectDataReader.Factory<TestClass> Factory;

		public ObjectDataReaderBenchmarks()
		{
			ObjectDataReader.BuildFactory<TestClass>
		}

		[Benchmark]
		public void ObjectDataReader()
		{

		}

	}
}
