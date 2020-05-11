using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Sylvan.IO;
using System.IO;

namespace Sylvan.Benchmarks
{
	[SimpleJob(RuntimeMoniker.NetCoreApp31, launchCount: 1, warmupCount: 3, targetCount: 4)]
	[MemoryDiagnoser]
	public class MemoryStreamBenchmarks
	{
		const int Iterations = 100;

		[Params(100, 500, 1000, 5000)]
		public int Count { get; set; }

		[Benchmark(Baseline = true)]
		public void MemoryStream()
		{
			for (int i = 0; i < Iterations; i++)
			{
				using var stream = new MemoryStream(200);
				Process(stream);
			}
		}

		[Benchmark]
		public void BlockMemoryStream()
		{
			for (int i = 0; i < Iterations; i++)
			{
				using var stream = new BlockMemoryStream();
				Process(stream);
			}
		}

		void Fill(Stream stream)
		{
			var tw = new StreamWriter(stream);
			for (int i = 0; i < Count; i++)
			{
				tw.Write("This is a string");
				tw.Write(12345);
			}

			stream.Seek(0, SeekOrigin.Begin);
		}

		void Process(Stream stream)
		{
			Fill(stream);
			stream.CopyTo(Stream.Null);
		}

	}
}
