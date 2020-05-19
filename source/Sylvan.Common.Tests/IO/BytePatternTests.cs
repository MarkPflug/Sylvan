using System.Linq;
using System.Text;
using Xunit;

namespace Sylvan.IO
{
	public class BytePatternTests
	{
		[Fact]
		public void Test1()
		{
			var pat = new BytePattern(Encoding.ASCII.GetBytes("a search string"));

			var data = Encoding.ASCII.GetBytes("this is a longer piece of text containing \"a search string\", and it contains \"a search string\" multiple times");

			var results = pat.SearchAll(data);
			Assert.Equal(2, results.Count());
		}
	}
}
