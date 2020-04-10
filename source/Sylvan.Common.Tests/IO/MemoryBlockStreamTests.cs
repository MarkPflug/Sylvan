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
			var f = new BlockMemoryStreamFactory();
			var s = f.Create();

			var data = Encoding.ASCII.GetBytes("This is a piece of short test data");
			for(int i = 0; i < 10000; i++)
			{
				s.Write(data);
			}

			s.Seek(0, System.IO.SeekOrigin.Begin);
			var r = new StreamReader(s, Encoding.ASCII);
			char[] cb = new char[0x1000];
			var l = r.Read(cb, 0, cb.Length);
		}
	}
}
