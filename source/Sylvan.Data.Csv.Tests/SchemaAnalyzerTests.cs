using System.IO;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class SchemaAnalyzerTests
	{
		[Fact]
		public void Test1()
		{
			var data = "Id,Name,Col1,Col2,Col3\r\n1,Test,1,2,3";
			var csv = CsvDataReader.Create(new StringReader(data));
			var opts = new SchemaAnalyzerOptions { DetectSeries = true };
			var a = new SchemaAnalyzer(opts);
			var result = a.Analyze(csv);

			var schema = result.GetSchema().GetColumnSchema();
			Assert.Equal(3, schema.Count);
			Assert.Equal("Id", schema[0].ColumnName);
			Assert.Equal("Name", schema[1].ColumnName);
			Assert.Equal("Col*", schema[2].ColumnName);
			Assert.Equal(typeof(int), schema[2].DataType);
		}

		[Fact]
		public void Test2()
		{
			var data = "Id,Name,Col1,Col2,Col3\r\n1,Test,1,2,3.77";
			var csv = CsvDataReader.Create(new StringReader(data));

			var opts = new SchemaAnalyzerOptions { DetectSeries = true };
			var a = new SchemaAnalyzer(opts);
			var result = a.Analyze(csv);

			var schema = result.GetSchema().GetColumnSchema();
			Assert.Equal(3, schema.Count);
			Assert.Equal("Id", schema[0].ColumnName);
			Assert.Equal("Name", schema[1].ColumnName);
			Assert.Equal("Col*", schema[2].ColumnName);
			Assert.Equal(typeof(double), schema[2].DataType);
		}
	}
}
