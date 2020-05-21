using System;
using Xunit;

namespace Sylvan.IO
{
	public class RandomStreamTests
	{
		[Fact]
		public void Test1()
		{
			var rs = new RandomStream(0x100000);
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
			var rs = new RandomStream(r, 0x1000);
			byte[] buffer = new byte[0x2000];
			var l = rs.Read(buffer, 0, buffer.Length);
			Assert.Equal(0x1000, l);
			var c = 0;
			for (int i = 0; i < l; i++)
			{
				c += buffer[i] == 0 ? 1 : 0;
			}
			Assert.True(c < 0x1000);
		}
	}
}
