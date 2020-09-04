using BenchmarkDotNet.Attributes;

namespace Sylvan.Benchmarks
{
	public class NumberBenchmarks
	{
		[Benchmark(Baseline = true)]
		public void IntToString()
		{
			for (int i = 0; i < 100; i++)
			{
				var s = i.ToString();
			}
		}

		[Benchmark]
		public void IntToStringCommon()
		{
			for (int i = 0; i < 100; i++)
			{
				var s = i.ToStringCommon();
			}
		}
	}
}
