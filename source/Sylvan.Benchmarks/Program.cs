using BenchmarkDotNet.Running;

namespace Sylvan.Benchmarks
{
	static class Program
	{
		public static void Main()
		{
			var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run();
		}
	}
}