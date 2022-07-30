using Sylvan.Data.Csv;
using System;
using System.Data.Common;
using System.IO;
using Xunit;

namespace Sylvan.Data;

public class SchemaValidatingDataReaderTests
{

	static object[] defaults = new object[] { -1, Guid.Empty, null };

	static bool Validate(DbDataReader data, int ordinal, out object value)
	{
		value = defaults[ordinal];
		return true;
	}

	static bool Fail(DbDataReader data, int ordinal, out object value)
	{
		value = null;
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
}
