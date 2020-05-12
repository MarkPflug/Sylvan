using Sylvan.Data.Csv;
using System.Data.Common;
using System.IO;
using Xunit;

namespace Sylvan.Data
{
	public class SchemaTests
	{
		[Fact]
		public void Test1()
		{
			var data = TestData.GetTestData();
			var schema = data.GetColumnSchema();
			var sw = new StringWriter();
			DataSchema.Write(sw, schema);
			var str = sw.ToString();
		}

		[Fact]
		public void Test2()
		{
			using var tr = File.OpenText("Data/Test.csv");
			var data = CsvDataReader.Create(tr);
			var schema = data.GetColumnSchema();
			var sa = new SchemaAnalyzer();
			var result = sa.Analyze(data);
			var analyzedSchema = result.GetColumnSchema();
			var sw = new StringWriter();
			DataSchema.Write(sw, analyzedSchema);
			var str = sw.ToString();
		}
	}
}
