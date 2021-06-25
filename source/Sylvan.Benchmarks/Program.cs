using BenchmarkDotNet.Running;
using System.Threading.Tasks;

namespace Sylvan.Benchmarks
{
	static class Program
	{
		public static void Main(string[] args)
		{
			BenchmarkSwitcher
				.FromAssembly(typeof(Program).Assembly)
				.Run(args);
		}
	}
}