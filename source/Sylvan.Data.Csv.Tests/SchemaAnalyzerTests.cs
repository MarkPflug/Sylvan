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
		public void TestDateSeries()
		{
			var data = "Name,08/12/20,08/13/20,08/14/20\r\nTest,1,2,3";
			var csv = CsvDataReader.Create(new StringReader(data));
			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);

			var schema = result.GetSchema();
			Assert.Equal(2, schema.Count);
			Assert.Equal("Name", schema[0].ColumnName);
			Assert.Null(schema[1].ColumnName);
			Assert.Equal(typeof(int), schema[1].DataType);

		}
	}
}
