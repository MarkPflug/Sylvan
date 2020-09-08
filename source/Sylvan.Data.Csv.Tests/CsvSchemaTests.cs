using System.IO;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvSchemaTests
	{
		[Fact]
		public void Test1()
		{
			var data = TestData.GetTestDataReader();
			var schema = new Schema(data);
			var spec = schema.GetSchemaSpecification(true);
		}

		[Fact]
		public void ParseTest1()
		{
			var spec = Schema.TryParse("A:Int,B:String?");
			Assert.NotNull(spec);
		}

		[Fact]
		public void Variadic()
		{
			var spec = Schema.TryParse("Id:string,*:int");
			var data = "Id,8/12/20,8/13/20,8/14/20\r\nTest,1,2,3\r\nTest2,12345,54321,2343";
			var schema = new CsvSchema(spec.GetColumnSchema());
			var csv = CsvDataReader.Create(new StringReader(data), new CsvDataReaderOptions { Schema = schema });
			var csvSchema = csv.GetColumnSchema();
			var name = csv.GetName(2);
			Assert.Equal("8/13/20", name);
			while (csv.Read())
			{
				csv.GetInt32(2);
				csv.GetInt32(3);
			}
		}
	}
}
