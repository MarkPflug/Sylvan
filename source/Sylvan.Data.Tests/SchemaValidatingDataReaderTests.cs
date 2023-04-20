using Sylvan.Data.Csv;
using System;
using System.IO;
using System.Linq;
using Xunit;

namespace Sylvan.Data;

public class SchemaValidatingDataReaderTests
{
	// uses preview features, which are marked obsolete.
#pragma warning disable CS0618

	static object[] defaults = new object[] { -1, Guid.Empty, null };

	static bool Validate(DataValidationContext context)
	{
		foreach(var error in context.GetErrors())
		{
			context.SetValue(error, defaults[error]);
		}
		return true;
	}

	static bool Fail(DataValidationContext context)
	{
		return false;
	}


	[Fact]
	public void Test1()
	{
		var schema = Schema.Parse(":int,:string,:DateTime");
		var opts = new CsvDataReaderOptions { Schema = new CsvSchema(schema) };
		var data = "A,B,C\n1,Test,2022-01-01\n";
		var csv = CsvDataReader.Create(new StringReader(data), opts);


		var r = csv.ValidateSchema(Fail);
		Assert.True(r.Read());
		Assert.Equal(1, r.GetInt32(0));
		Assert.Equal("Test", r.GetString(1));
		Assert.Equal(new DateTime(2022,1,1), r.GetDateTime(2));
		Assert.False(r.Read());
	}
	
	[Fact]
	public void Test2()
	{
		var schema = Schema.Parse(":int,:Guid,:DateTime");
		var opts = new CsvDataReaderOptions { Schema = new CsvSchema(schema) };
		var data = "A,B,C\na,,nope";
		var csv = CsvDataReader.Create(new StringReader(data), opts);

		var r = csv.ValidateSchema(Validate);
		Assert.True(r.Read());
		Assert.False(r.IsDBNull(0));
		Assert.Equal(-1, r.GetInt32(0));
		Assert.False(r.IsDBNull(1));
		Assert.Equal(Guid.Empty, r.GetGuid(1));
		Assert.True(r.IsDBNull(2));
	}

	class Validator
	{
		internal int count = 0;

		public bool Validate(DataValidationContext context)
		{
			count++;
			return context.GetErrors().Any();
		}
	}

	[Fact]
	public void ValidateAllRows()
	{
		var schema = Schema.Parse(":int,:double,:DateTime");
		var opts = new CsvDataReaderOptions { Schema = new CsvSchema(schema) };
		// one row matches the schema, but we expect 3 calls to validate
		var data = "A,B,C\na,,nope\n1,1.1,2020-12-12\nx,y,z";
		var csv = CsvDataReader.Create(new StringReader(data), opts);

		var v = new Validator();

		var r = csv.Validate(v.Validate);
		// process rows
		while (r.Read());

		Assert.Equal(3, v.count);

	}
}
