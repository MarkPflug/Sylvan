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

	public class CustomDbColumn : DbColumn
	{
		public new bool? AllowDBNull { get => base.AllowDBNull; set => base.AllowDBNull = value; }
		public new string ColumnName { get => base.ColumnName; set => base.ColumnName = value; }

		public new Type DataType
		{
			get => base.DataType;
			set
			{
				//it works correctly after uncommenting below code
				//
				//var underlying = System.Nullable.GetUnderlyingType(value);
				//if (underlying != null)
				//{
				//	AllowDBNull = true;
				//	base.DataType = underlying;
				//	return;
				//}
				//base.DataType = value;
			}
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
			new CustomDbColumn() { ColumnName = "Id", DataType = typeof(int) },
			new CustomDbColumn() { ColumnName = "Name", DataType = typeof(string), AllowDBNull = true },
			new CustomDbColumn() { ColumnName = "Date", DataType = typeof(DateTime?) },
			new CustomDbColumn() { ColumnName = "Amount", DataType = typeof(decimal?) },
			new CustomDbColumn() { ColumnName = "Duration", DataType = typeof(TimeSpan?) },
			new CustomDbColumn() { ColumnName = "Value", DataType = typeof(double?) },
			new CustomDbColumn() { ColumnName = "Count", DataType = typeof(int?) }
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
