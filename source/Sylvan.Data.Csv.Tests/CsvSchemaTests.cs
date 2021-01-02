using System.IO;
using System;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvSchemaTests
	{
		[Fact]
		public void SchemaTest()
		{
			var sb = new 
				Schema
				.Builder()
				;

			var data = TestData.GetTestDataReader();
			var schema = new Schema(data);
			var spec = schema.ToString();
		}

		[Fact]
		public void Test1()
		{
			var data = TestData.GetTestDataReader();
			var schema = new Schema(data);
			var spec = schema.ToString();
		}

		[Fact]
		public void ParseTest1()
		{
			var spec = SchemaSerializer.Simple.Read("A:Int,B:String?");
			Assert.NotNull(spec);
		}

		[Fact]
		public void ParseTest2()
		{
			var spec = SchemaSerializer.Simple.Read("A,B");
			Assert.NotNull(spec);
			var cols = spec.GetColumnSchema();
			Assert.Equal("A", cols[0].ColumnName);
			Assert.Equal(typeof(string), cols[0].DataType);
			Assert.False(cols[0].AllowDBNull);

			Assert.Equal("B", cols[1].ColumnName);
			Assert.Equal(typeof(string), cols[1].DataType);
			Assert.False(cols[1].AllowDBNull);
		}

		[Fact]
		public void ParseTestFormat()
		{
			var spec = SchemaSerializer.Simple.Read("Name,Date:DateTime{yyyyMMdd}");
			Assert.NotNull(spec);
			var cols = spec.GetColumnSchema();
			Assert.Equal("Name", cols[0].ColumnName);
			Assert.Equal(typeof(string), cols[0].DataType);

			Assert.Equal("Date", cols[1].ColumnName);
			Assert.Equal(typeof(DateTime), cols[1].DataType);
			Assert.Equal("yyyyMMdd", cols[1]["Format"]);
		}

		[Fact]
		public void SeriesSchema()
		{
			var spec = SchemaSerializer.Simple.Read("Id:string,Cases*:int");
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
