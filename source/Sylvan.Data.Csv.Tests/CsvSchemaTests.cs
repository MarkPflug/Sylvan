using System.Data.Common;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvSchemaTests
	{
		[Fact]
		public void Test1()
		{
			var data = TestData.GetTestDataReader();
			var schema = new CsvSchema(data.GetColumnSchema());
			var spec = schema.GetSchemaSpecification(true);

		}


		[Fact]
		public void ParseTest1()
		{
			var spec = CsvSchema.TryParse("A:Int,B:String?");
			Assert.NotNull(spec);
		}
	}
}
