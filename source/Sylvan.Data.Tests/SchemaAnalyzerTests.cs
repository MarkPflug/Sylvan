using Sylvan.Data.Csv;
using System.IO;
using Xunit;

namespace Sylvan.Data
{
	public class SchemaAnalyzerTests
	{
		[Fact]
		public static void StringSchema()
		{
			var data = TestData.GetData();
			var a = new SchemaAnalyzer();
			var result = a.Analyze(data);
			var schema = new Schema(result.GetSchema());
			var spec = schema.ToString();

			var ss = Schema.Parse(spec);
			Assert.NotNull(ss);
		}

		[Fact]
		public static void TypedSchema()
		{
			var data = TestData.GetTestData();
			var a = new SchemaAnalyzer();
			var result = a.Analyze(data);
			var schema = new Schema(result.GetSchema());
			var spec = schema.ToString();

			var ss = Schema.Parse(spec);
			Assert.NotNull(ss);
		}

		[Fact]
		public static void Boolean()
		{
			var data = "a,b,c,d\n1,name,T,F\n2,Test,F,F\n3,Foo,,T\n";
			using var csv = CsvDataReader.Create(new StringReader(data));

			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);
			var schema = new Schema(result.GetSchema());
			var spec = schema.ToString();

			var ss = Schema.Parse(spec);
			Assert.NotNull(ss);
		}

		[Fact]
		public static void BooleanInt()
		{
			var data = "a,b,c,d\n1,name,1,0\n2,Test,0,0\n3,Foo,,1\n";
			using var csv = CsvDataReader.Create(new StringReader(data));

			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);
			var schema = new Schema(result.GetSchema());
			var spec = schema.ToString();

			var ss = Schema.Parse(spec);
			Assert.NotNull(ss);
		}

		[Fact]
		public static void UnknownType()
		{
			var data = "a,b\n1,\n2,\n3,\n";
			using var csv = CsvDataReader.Create(new StringReader(data));

			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);
			var schema = result.GetSchema();
			var cols = schema.GetColumnSchema();
			Assert.Equal(typeof(int), cols[0].DataType);
			Assert.Equal(typeof(string), cols[1].DataType);
			Assert.Equal(true, cols[1].AllowDBNull);
			Assert.Equal((int?)null, cols[1].ColumnSize);
		}

		[Fact]
		public static void StringSize()
		{
			var data = "a,b\n1,asdf\n2,qwer\n3,zxcv\n";
			using var csv = CsvDataReader.Create(new StringReader(data));

			var a = new SchemaAnalyzer();
			var result = a.Analyze(csv);
			var schema = result.GetSchema();
			var cols = schema.GetColumnSchema();
			Assert.Equal(typeof(int), cols[0].DataType);
			Assert.Equal(typeof(string), cols[1].DataType);
			Assert.Equal(false, cols[1].AllowDBNull);
			Assert.Equal(4, cols[1].ColumnSize);
		}
	}
}
