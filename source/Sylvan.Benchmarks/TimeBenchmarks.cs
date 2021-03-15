using BenchmarkDotNet.Attributes;
using Sylvan.Diagnostics;
using System;

namespace Sylvan.Benchmarks
{
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
		public void TimerTest2()
		{
			var s = timer.Start();
			s.Dispose();
		}

		[Benchmark]
		public void TimerTest3()
		{
			using (var s = timer.Start()) { }
		}

		[Benchmark]
		public void CounterTest()
		{
			counter.Increment();
		}
	}
}
