using System;
using System.IO;
using System.Text;
using Xunit;

namespace Sylvan.IO
{
	public class MemoryBlockStreamTests
	{

		[Fact]
		public void Test1()
		{
			var s = new PooledMemoryStream();

			const string testStr = "This is a piece of short test data";
			var data = Encoding.ASCII.GetBytes(testStr);
			const int Count = 100;
			for (int i = 0; i < Count; i++)
			{
				s.Write(data, 0, data.Length);
			}

			s.Seek(0, SeekOrigin.Begin);
			var r = new StreamReader(s, Encoding.ASCII);
			var str = r.ReadToEnd();
			Assert.Equal(data.Length * Count, str.Length);
		}

//#if NET5_0
//		[Fact]
//		public void Test2()
//		{
//			var msb = new Sylvan.Benchmarks.MemoryStreamBenchmarks();
//			msb.PooledMemoryStream();
//		}
//#endif
	}
}
