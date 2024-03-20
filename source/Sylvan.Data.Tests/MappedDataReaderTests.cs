using Sylvan.Data.Csv;
using System;
using System.Data.Common;
using System.IO;
using Xunit;

namespace Sylvan.Data;

static class Ex
{
	public static IDataColumn Col(this DbDataReader r, int ordinal)
	{
		return new PassthroughColumn(r, ordinal);
	}
}

public class MappedDataReaderTests
{
	[Fact]
	public void WithColumns1()
	{
		var schema = Schema.Parse("Name:string,Number:int,Date:date");
		var opts = new CsvDataReaderOptions { Schema = new CsvSchema(schema) };
		var csv = new StringReader("a,b,c\na,1010,2022-01-01\nb,1020,2022-01-02");
		CsvDataReader csvReader = CsvDataReader.Create(csv, opts);
		DateTime importDate = new DateTime(2022, 1, 3);
		var data = csvReader.WithColumns(
			new CustomDataColumn<DateTime>("ImportDate", r => importDate),
			new CustomDataColumn<int>("RowNum", r => csvReader.RowNumber)
		);
		var sw = new StringWriter();
		var wo = new CsvDataWriterOptions { NewLine = "\n", DateTimeFormat = "yyyy-MM-dd" };
		var csvWriter = CsvDataWriter.Create(sw, wo);
		csvWriter.Write(data);
		Assert.Equal("Name,Number,Date,ImportDate,RowNum\na,1010,2022-01-01,2022-01-03,1\nb,1020,2022-01-02,2022-01-03,2\n", sw.ToString());
	}

	[Fact]
	public void WithColumns2()
	{
		var data = "a,b,c\n\"1\",NULL,\"3\"\n";
		var csv = CsvDataReader.Create(new StringReader(data));
		var r = csv.WithColumns(new CustomDataColumn<int?>("b", r => IsNullString(r, 1) ? null : r.GetInt32(1)));
		r = r.Select(0, 3, 2);

		Assert.True(r.Read());
		Assert.Equal(1, r.GetInt32(0));
		Assert.True(r.IsDBNull(1));
		Assert.Equal(3, r.GetInt32(2));
		Assert.False(r.Read());

	}

	[Fact]
	public void TestGetData()
	{
		var data = "a\n1";
		var csv = CsvDataReader.Create(new StringReader(data));
		var r = csv.WithColumns(new CustomDataColumn<byte[]>("b", r => new byte[] { 1, 2, 3 }));

		Assert.True(r.Read());
		Assert.Equal(1, r.GetInt32(0));

		Assert.Equal(3, r.GetBytes(1, 0, null, 0, 0));
		var buf = new byte[3];
		Assert.Equal(3, r.GetBytes(1, 0, buf, 0, buf.Length));
		Assert.Equal(new byte[] { 1, 2, 3 }, buf);
		Assert.False(r.Read());
	}

	static bool IsNullString(DbDataReader r, int idx)
	{
		var s = r.GetString(idx);
		return StringComparer.OrdinalIgnoreCase.Equals("null", s);
	}

	[Fact]
	public void Select()
	{
		var data =
			"""
			a,b,c
			1,2,3
			4,5,6
			""";
		var r = new StringReader(data);

		var csv = CsvDataReader.Create(r);

		var dr = csv.Select(
			new CustomDataColumn<int>("Row#", r => csv.RowNumber),
			csv.Col(0),
			csv.Col(1),
			new CustomDataColumn<int>("Total", r => csv.GetInt32(0) + csv.GetInt32(1) + csv.GetInt32(2))
		);

		var ww = new StringWriter();
		using (var cw = CsvDataWriter.Create(ww)) {
			cw.Write(dr);
		}
		var str = ww.ToString();
	}
}

class PassthroughColumn : IDataColumn
{
	readonly DbDataReader r;
	readonly int ordinal;

	public PassthroughColumn(DbDataReader r, int ordinal)
	{
		this.r = r;
		this.ordinal = ordinal;
	}

	public string Name => r.GetName(ordinal);

	public Type ColumnType => r.GetFieldType(ordinal);

	public bool AllowNull => r.GetColumnSchema()[ordinal].AllowDBNull != false;

	public int GetData<T>(DbDataReader reader, T[] buffer, long dataOffset, int bufferOffset, int length)
	{
		throw new NotImplementedException();
	}

	public object GetValue(DbDataReader reader)
	{
		return r.GetValue(ordinal);
	}

	public T GetValue<T>(DbDataReader reader)
	{
		return r.GetFieldValue<T>(ordinal);
	}

	public bool IsDbNull(DbDataReader reader)
	{
		return r.IsDBNull(ordinal);
	}
}
