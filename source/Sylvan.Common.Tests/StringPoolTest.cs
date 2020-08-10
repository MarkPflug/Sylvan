//using Xunit;

//namespace Sylvan
//{
//	public class StringPoolTest
//	{

//		[Fact]
//		public void Test1()
//		{
//			var sp = new StringPool(32);
//			var data = new char[] { 'a', 'b', 'c', 'd' };
//			var str = sp.GetString(data);
//			Assert.NotNull(str);
//			Assert.Equal("abcd", str);
//			var str2 = sp.GetString(data);
//			Assert.Same(str, str2);
//		}

//		[Fact]
//		public void Test2()
//		{
//			var sp = new StringPoolFast(32);
//			var data = new char[] { 'a', 'b', 'c', 'd' };
//			var str = sp.GetString(data);
//			Assert.NotNull(str);
//			Assert.Equal("abcd", str);
//			var str2 = sp.GetString(data);
//			Assert.Same(str, str2);
//		}
//	}
//}
