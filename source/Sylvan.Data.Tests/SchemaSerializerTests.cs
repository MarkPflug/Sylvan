using Xunit;

namespace Sylvan.Data
{
	public class SchemaSerializerTests
	{
		[Fact]
		public void Test1()
		{
			var schema = SchemaSerializer.Simple.Read("{Date}>Values*:int");
		}
	}
}
