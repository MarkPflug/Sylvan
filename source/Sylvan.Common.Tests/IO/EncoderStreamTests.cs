using System;
using System.IO;
using System.Text;
using Xunit;

namespace Sylvan.IO
{
	public class EncoderStreamTests
	{
		byte[] inputData;

		public EncoderStreamTests()
		{
			var sw = new StringWriter();
			for (int i = 0; i < 100; i++)
			{
				sw.WriteLine($"{i}: abcdefghijklmnopqrstuvwxyz.");
			}
			inputData = Encoding.ASCII.GetBytes(sw.ToString());
		}

		[Fact]
		public void Test1()
		{
			var ms = new MemoryStream();
			var enc = new Base64Encoder();
			var s = new EncoderStream(ms, enc);
			s.Write(inputData, 0, inputData.Length);
			s.Flush();
			s.Close();
			ms.Position = 0;
			var str = Encoding.ASCII.GetString(ms.GetBuffer(), 0, (int) ms.Length);
			var data = Convert.FromBase64String(str);
			var debug = Encoding.ASCII.GetString(data);

			Assert.Equal(inputData, data);
		}
	}
}