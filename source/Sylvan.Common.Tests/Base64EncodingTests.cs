using System.Text;
using Xunit;

namespace Sylvan
{
	public class Base64EncodingTests
	{
		[Fact]
		public void Test1()
		{
			var ascii = Encoding.ASCII;
			var enc = Base64Encoding.Default;
			var result = enc.Encode(ascii.GetBytes("Devs"));
			Assert.Equal("RGV2cw==", result);
		}
	}
}
