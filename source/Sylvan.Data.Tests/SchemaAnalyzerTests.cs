﻿using Xunit;

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
			var schema = new Schema(a.GetSchema(result));
			var spec = schema.GetSchemaSpecification(true);

			var ss = Schema.TryParse(spec);
			Assert.NotNull(ss);
		}

		[Fact]
		public static void TypedSchema()
		{
			var data = TestData.GetTestData();
			var a = new SchemaAnalyzer();
			var result = a.Analyze(data);
			var schema = new Schema(a.GetSchema(result));
			var spec = schema.GetSchemaSpecification(true);


			var ss = Schema.TryParse(spec);
			Assert.NotNull(ss);
		}
	}
}