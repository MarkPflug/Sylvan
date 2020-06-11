using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Sylvan.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Sylvan.Benchmarks
{
	[SimpleJob(RuntimeMoniker.NetCoreApp31)]
	[MemoryDiagnoser]
	public class HexEncoderBenchmarks
	{
		byte[] inputData;

		public HexEncoderBenchmarks()
		{
			var sw = new StringWriter();
			for (int i = 0; i < 1000; i++)
			{
				sw.WriteLine($"{i}: abcdefghijklmnopqrstuvwxyz");
			}
			this.inputData = Encoding.ASCII.GetBytes(sw.ToString());
		}

		[Benchmark(Baseline = true)]
		public void BCL()
		{
			var str = BitConverter.ToString(inputData);
		}
				
		[Benchmark]
		public void SylvanEncoderStream()
		{
			var ms = new PooledMemoryStream();
			var es = new EncoderStream(ms, new HexEncoder());
			es.Write(inputData, 0, inputData.Length);
			es.Close();
		}
	}
}
