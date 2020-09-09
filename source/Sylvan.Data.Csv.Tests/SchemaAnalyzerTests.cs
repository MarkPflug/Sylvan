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
			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);

			var schema = result.GetSchema();
			Assert.Equal(3, schema.Count);
			Assert.Equal("Id", schema[0].ColumnName);
			Assert.Equal("Name", schema[1].ColumnName);
			Assert.Null(schema[2].ColumnName);
			Assert.Equal(typeof(int), schema[2].DataType);
		}

		[Fact]
		public void Test2()
		{
			var data = "Id,Name,Col1,Col2,Col3\r\n1,Test,1,2,3.77";
			var csv = CsvDataReader.Create(new StringReader(data));
			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);

			var schema = result.GetSchema();
			Assert.Equal(3, schema.Count);
			Assert.Equal("Id", schema[0].ColumnName);
			Assert.Equal("Name", schema[1].ColumnName);
			Assert.Null(schema[2].ColumnName);
			Assert.Equal(typeof(float), schema[2].DataType);
		}

		[Fact]
		public void DateSeriesTest()
		{
			var data = TestData.GetTextReader();
			var csv = CsvDataReader.Create(data);
			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);

			var schema = result.GetSchema();
			var col = schema[11];
			Assert.NotNull(col);
		}
	}
}
