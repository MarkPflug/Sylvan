using System;
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
		public void ParseTest2()
		{
			var spec = Schema.TryParse("Abra,Cadabra");
			Assert.NotNull(spec);
			var cols = spec.GetColumnSchema();
			Assert.Equal("Abra", cols[0].ColumnName);
			Assert.Equal(typeof(string), cols[0].DataType);

			Assert.Equal("Cadabra", cols[1].ColumnName);
			Assert.Equal(typeof(string), cols[1].DataType);
		}

		[Fact]
		public void ParseTestFormat()
		{
			var spec = Schema.TryParse("Name,Date:DateTime{yyyyMMdd}");
			Assert.NotNull(spec);
			var cols = spec.GetColumnSchema();
			Assert.Equal("Name", cols[0].ColumnName);
			Assert.Equal(typeof(string), cols[0].DataType);

			Assert.Equal("Date", cols[1].ColumnName);
			Assert.Equal(typeof(DateTime), cols[1].DataType);
			Assert.Equal("yyyyMMdd", cols[1]["Format"]);
		}
	}
}
