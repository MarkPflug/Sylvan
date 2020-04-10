using Xunit;

namespace Sylvan
{
	public class NumberTests
	{
		[Fact]
		public void Test1()
		{
			var v = Number.ParseInt("1234");
			Assert.Equal(1234, v);
		}
	}
}
