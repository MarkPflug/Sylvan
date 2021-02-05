using BenchmarkDotNet.Running;

namespace Sylvan.Benchmarks
{
	static class Program
	{
		public static void Main(string[] args)
		{
			//new MemoryStreamBenchmarks().PooledMemoryStreamBench();
			var summary = BenchmarkSwitcher
				.FromAssembly(typeof(Program).Assembly).Run(args);
		}
	}
}