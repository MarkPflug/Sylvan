using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System;
using System.Diagnostics;

namespace Sylvan.Benchmarks
{

	[SimpleJob(RuntimeMoniker.NetCoreApp31)]
	public class TimeBenchmarks
	{
		[Benchmark(Baseline = true)]
		public void UtcNow()
		{
			var v = DateTime.UtcNow;
		}

		[Benchmark]
		public void Now()
		{
			var v = DateTime.Now;
		}

		[Benchmark]
		public void GetTimestamp()
		{
			var v = Stopwatch.GetTimestamp();
		}
	}
}
