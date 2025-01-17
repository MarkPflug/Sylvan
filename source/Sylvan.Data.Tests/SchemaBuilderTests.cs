using Sylvan.Data.Csv;
using System;
using System.Data.Common;
using System.IO;
using Xunit;

namespace Sylvan.Data;

public class SchemaBuilderTests
{
	[Fact]
	public void SchemaBuilderNullabilityTest()
	{
		var schema =
			new Schema.Builder()
			.Add<int?>("Id")
			.Add<double?>("Value")
			.Add<long>("Count")
			.Build();

		Assert.True(schema[0].AllowDBNull);
		Assert.Equal(typeof(int), schema[0].DataType);

		Assert.True(schema[1].AllowDBNull);
		Assert.Equal(typeof(double), schema[1].DataType);

		Assert.False(schema[2].AllowDBNull);
		Assert.Equal(typeof(long), schema[2].DataType);
	}

	class CustomDbColumn : DbColumn
	{
		public CustomDbColumn(string name, Type dataType)
		{
			this.ColumnName = name;
			this.DataType = dataType;
			var under = Nullable.GetUnderlyingType(dataType);
			this.DataType =  under ?? dataType;
			this.AllowDBNull = under != null;
		}

		public CustomDbColumn(string name, Type dataType, bool allowNull)
		{
			this.ColumnName = name;
			this.DataType = dataType;
			var under = Nullable.GetUnderlyingType(dataType);
			this.DataType = under ?? dataType;
			this.AllowDBNull = allowNull;
		}
	}

	[Fact]
	public void Nullability()
	{
		var data =
			"""
			Id,Name,Date,Amount,Duration,Value,Count
			1,Test,2022-11-13,123.45,00:32:59.99,11.22,11
			2,,,,,,,,,,,,,,,,,
			""";

		var columns = new System.Collections.Generic.List<CustomDbColumn>
		{
			new CustomDbColumn("Id", typeof(int)),
			new CustomDbColumn("Name", typeof(string), true),
			new CustomDbColumn("Date", typeof(DateTime?)),
			new CustomDbColumn("Amount", typeof(decimal?)),
			new CustomDbColumn("Duration", typeof(TimeSpan?)),
			new CustomDbColumn("Value", typeof(double?)),
			new CustomDbColumn("Count", typeof(int?))
		};

		var sb = new Schema.Builder(columns);

		var schema = new CsvSchema(sb.Build());
		using var text = new StringReader(data);
		using var edr = CsvDataReader.Create(text, new CsvDataReaderOptions { Schema = schema });

		Assert.True(edr.Read());
		var aa = edr.GetValue(0);
		// The first row contains non-null values
		Assert.Equal(1, edr.GetValue(0));
		Assert.Equal("Test", edr.GetValue(1));
		var val = edr.GetValue(2);
		Assert.Equal(new DateTime(2022, 11, 13), val);
		Assert.Equal(123.45m, edr.GetValue(3));
		Assert.Equal(new TimeSpan(0, 0, 32, 59, 990), edr.GetValue(4));
		Assert.Equal(11.22, edr.GetValue(5));
		Assert.Equal(11, edr.GetValue(6));

		Assert.True(edr.Read());
		// The second row contains all null values, except ID
		Assert.Equal(2, edr.GetValue(0));
		Assert.Equal(DBNull.Value, edr.GetValue(1));
		Assert.Equal(DBNull.Value, edr.GetValue(2));
		Assert.Equal(DBNull.Value, edr.GetValue(3));
		Assert.Equal(DBNull.Value, edr.GetValue(4));
		Assert.Equal(DBNull.Value, edr.GetValue(5));
		Assert.Equal(DBNull.Value, edr.GetValue(6));
	}
}
