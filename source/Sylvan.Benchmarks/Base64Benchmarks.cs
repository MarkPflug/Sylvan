using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Sylvan.IO;
using System;
using System.IO;
using System.Text;

namespace Sylvan.Benchmarks
{
	[SimpleJob(RuntimeMoniker.NetCoreApp31)]
	[MemoryDiagnoser]
	public class Base64Benchmarks
	{
		byte[] inputData;
		char[] outputChars;

		public Base64Benchmarks()
		{
			var sw = new StringWriter();
			for (int i = 0; i < 1000; i++)
			{
				sw.WriteLine($"{i}: abcdefghijklmnopqrstuvwxyz");
			}
			this.inputData = Encoding.ASCII.GetBytes(sw.ToString());
			this.outputChars = new char[inputData.Length * 4];
		}

		[Benchmark(Baseline = true)]
		public void BCL()
		{
			Convert.ToBase64CharArray(inputData, 0, inputData.Length, outputChars, 0);
		}

		[Benchmark]
		public void SylvanEncoderStream()
		{
			using var ms = new PooledMemoryStream();
			var es = new EncoderStream(ms, new Base64Encoder());
			es.Write(inputData, 0, inputData.Length);
			es.Close();
		}

		[Benchmark]
		public void SylvanEncoding()
		{
			var enc = Base64Encoding.Default;
			enc.Encode(inputData, 0, outputChars, 0, inputData.Length);

		}
	}
}
