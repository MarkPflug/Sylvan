using Xunit;

namespace Sylvan
{
	public class StringPoolTest
	{
		[Fact]
		public void DeDupeTest()
		{
			var sp = new StringPool(32);
			var data = new char[] { 'a', 'b', 'c', 'd' };
			var str = sp.GetString(data, 0, 4);
			Assert.NotNull(str);
			Assert.Equal("abcd", str);
			var str2 = sp.GetString(data, 0, 4);
			Assert.Same(str, str2);
		}
	}
}
