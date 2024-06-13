using Sylvan.Data.Csv;
using System;
using System.IO;
using System.Linq;
using Xunit;

namespace Sylvan.Data;

public class ValidatingDataReaderTests
{
	// uses preview features, which are marked obsolete.
#pragma warning disable CS0618

	static readonly object[] Defaults = new object[] { -1, Guid.Empty, null };

	static bool Validate(DataValidationContext context)
	{
		foreach (var error in context.GetErrors())
		{
			context.SetValue(error, Defaults[error]);
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
		Assert.Equal(new DateTime(2022, 1, 1), r.GetDateTime(2));
		Assert.False(r.Read());
	}

	[Fact]
	public void Test2()
	{
		var schema = Schema.Parse(":int,:Guid,:DateTime?");
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

	[Fact]
	public void ValidationStringNullability()
	{
		var schema = Schema.Parse(":String?,:String");
		var opts = new CsvDataReaderOptions { Schema = new CsvSchema(schema) };
		var data = "A,B\na,b\n,b\na,";
		var csv = CsvDataReader.Create(new StringReader(data), opts);

		static bool Validate(DataValidationContext context)
		{
			return true;
		}

		var r = csv.ValidateSchema(Validate);
		Assert.True(r.Read());
		Assert.False(r.IsDBNull(0));
		Assert.Equal("a", r.GetString(0));
		Assert.False(r.IsDBNull(1));
		Assert.Equal("b", r.GetString(1));
		Assert.True(r.Read());
		Assert.True(r.IsDBNull(0));
		Assert.Throws<InvalidCastException>(() => r.GetString(0));
		Assert.False(r.IsDBNull(1));
		Assert.Equal("b", r.GetString(1));
		Assert.True(r.Read());
		Assert.False(r.IsDBNull(0));
		Assert.Equal("a", r.GetString(0));
		Assert.False(r.IsDBNull(1));
		Assert.Equal("", r.GetString(1));
		Assert.False(r.Read());
	}

	[Fact]
	public void ValidationStructNullability()
	{
		var schema = Schema.Parse(":int?,:int");
		var opts = new CsvDataReaderOptions { Schema = new CsvSchema(schema) };
		var data = "A,B\n1,2\n,2\n1,";
		var csv = CsvDataReader.Create(new StringReader(data), opts);

		static bool Validate(DataValidationContext context)
		{
			return false;
		}

		var r = csv.ValidateSchema(Validate);

		Assert.True(r.Read());
		Assert.False(r.IsDBNull(0));
		Assert.Equal(1, r.GetInt32(0));
		Assert.False(r.IsDBNull(1));
		Assert.Equal(2, r.GetInt32(1));


		Assert.True(r.Read());
		Assert.True(r.IsDBNull(0));
		Assert.Throws<InvalidCastException>(() => r.GetInt32(0));
		Assert.False(r.IsDBNull(1));
		Assert.Equal(2, r.GetInt32(1));

		// the last row is invalid, so will not be read
		//Assert.True(r.Read());
		//Assert.False(r.IsDBNull(0));
		//Assert.Equal(1, r.GetInt32(0));
		//Assert.False(r.IsDBNull(1)); // the schema says it can't be null...
		//Assert.Throws<InvalidCastException>(() => r.GetInt32(0)); // but the underlying value can't be converted

		Assert.False(r.Read());
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
		while (r.Read()) ;

		Assert.Equal(3, v.count);
	}

	[Fact]
	public void SetValueClearsError()
	{
		var schema = Schema.Parse(":int");
		var opts = new CsvDataReaderOptions { Schema = new CsvSchema(schema) };
		// one row matches the schema, but we expect 3 calls to validate
		var data = "A\n\n-1\nX";
		var csv = CsvDataReader.Create(new StringReader(data), opts);

		static bool Validate(DataValidationContext context)
		{
			if (!context.IsValid(0))
			{
				context.SetValue(0, -1);
			}
			Assert.True(context.IsValid(0));
			return true;
		}

		var r = csv.Validate(Validate);
		// process rows
		while (r.Read())
		{
			Assert.Equal(-1, r.GetInt32(0));
		}
	}
}
