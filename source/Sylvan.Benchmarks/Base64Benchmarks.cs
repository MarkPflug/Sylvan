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
	public class Base64Benchmarks
	{
		byte[] inputData;

		public Base64Benchmarks()
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
			var str = Convert.ToBase64String(inputData);
		}
		
		[Benchmark]
		public void SylvanRaw()
		{
			var enc = new Base64Encoding();
			var str = enc.Encode(inputData);
		}

		[Benchmark]
		public void SylvanEncoderStream()
		{
			var ms = new PooledMemoryStream();
			var es = new EncoderStream(ms, new Base64Encoder());
			es.Write(inputData, 0, inputData.Length);
			es.Close();
		}
	}
}
