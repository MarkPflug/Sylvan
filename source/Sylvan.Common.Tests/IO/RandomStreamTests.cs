using System;
using Xunit;

namespace Sylvan.IO
{
	public class RandomStreamTests
	{
		[Fact]
		public void Test1()
		{
			var rs = new RandomStream();
			byte[] buffer = new byte[0x1000];
			var l = rs.Read(buffer, 0, buffer.Length);
			Assert.Equal(0x1000, l);
			var c = 0;
			foreach (var b in buffer)
			{
				c += b == 0 ? 1 : 0;
			}
			Assert.True(c < 0x1000);
		}


		[Fact]
		public void Test2()
		{
			var r = new Random(1);
			var rs = new RandomStream(r, 100);
			byte[] buffer = new byte[0x1000];
			var l = rs.Read(buffer, 0, buffer.Length);
			Assert.Equal(0x1000, l);
			var c = 0;
			foreach (var b in buffer)
			{
				c += b == 0 ? 1 : 0;
			}
			Assert.True(c < 0x1000);
		}
	}
}
