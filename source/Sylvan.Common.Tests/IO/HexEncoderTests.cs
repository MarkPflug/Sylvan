using Sylvan.Benchmarks;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Sylvan.IO
{
	public class HexEncoderTests
	{
		[Fact]
		public void Test1()
		{
			var enc = new HexEncoder();
			var dst = new byte[7];
			var l = 0;
			var o = 0;
			var r = enc.Encode(new byte[] { 0, 1, 0xd2, 0xff }, dst, out l, out o);
			Assert.Equal(3, l);
			Assert.Equal(6, o);
			Assert.Equal(EncoderResult.RequiresOutputSpace, r);			
		}

		[Fact]
		public void Test2()
		{
			var enc = new HexEncoder();
			var dst = new byte[16];
			var l = 0;
			var o = 0;
			var r = enc.Encode(new byte[] { 0, 1, 0xd2, 0xff }, dst, out l, out o);
			Assert.Equal(4, l);
			Assert.Equal(8, o);
			Assert.Equal(EncoderResult.Flush, r);
		}

		[Fact]
		public void Test3()
		{
			new HexEncoderBenchmarks().SylvanEncoderStream();
		}
	}
}


