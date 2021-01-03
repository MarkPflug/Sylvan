using Xunit;

namespace Sylvan.Data
{
	public class SchemaSerializerTests
	{
		[Fact]
		public void Test1()
		{
			var schema = Schema.Parse("{Date}>Values*:int");
		}
	}
}
