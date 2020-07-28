using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Sylvan.Diagnostics;
using System;

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
			var v = System.Diagnostics.Stopwatch.GetTimestamp();
		}

		static PerformanceTimer timer = new PerformanceTimer("TestTimer");
		static PerformanceCounter counter = new PerformanceCounter("TestCounter");

		[Benchmark]
		public void TimerTest()
		{
			using var s = timer.Start();
		}

		[Benchmark]
		public void CounterTest()
		{
			counter.Increment();
		}
	}
}
