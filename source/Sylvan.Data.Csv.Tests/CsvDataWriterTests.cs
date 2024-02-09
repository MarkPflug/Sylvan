using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data.Csv;

public class CsvDataWriterTests
{
	// use \n for newlines to make assertions easier
	static readonly CsvDataWriterOptions TestOptions = new CsvDataWriterOptions { NewLine = "\n" };

	// a culture that uses ',' for numeric decimal separator
	static readonly CultureInfo ItalianCulture = CultureInfo.GetCultureInfoByIetfLanguageTag("it-IT");

	static string GetCsv<T>(IEnumerable<T> data, CsvDataWriterOptions opts = null) where T : class
	{
		var dr = data.AsDataReader();
		return GetCsv(dr, opts);
	}

	static string GetCsv(DbDataReader dr, CsvDataWriterOptions opts = null)
	{
		var sw = new StringWriter();
		var csv = CsvDataWriter.Create(sw, opts ?? TestOptions);
		csv.Write(dr);
		return sw.ToString();
	}

	[Fact]
	public void VariableColumn()
	{
		var csvData = "a,b,c\n1,2,3\n1,2,3,4\n1,2\n1\n,,,,5\n";
		var r =
			CsvDataReader.Create(new StringReader(csvData))
			.AsVariableField(r => r.RowFieldCount);
		var sw = new StringWriter();
		var cw = CsvDataWriter.Create(sw, new CsvDataWriterOptions { NewLine = "\n" });
		cw.Write(r);

		var str = sw.ToString();
		Assert.Equal(csvData, str);
	}

	[Fact]
	public void Simple()
	{
		var data = new[]
			{
				new
				{
					Boolean = true,
					Integer = int.MaxValue,
					Double = 15.25,
					Date = new DateTime(2020, 01, 01),
					Text = "Abcd",
				}
			};

		var csv = GetCsv(data);

		var r = CsvDataReader.Create(new StringReader(csv));
		r.Read();
		var expected = data[0];
		Assert.Equal(expected.Boolean, r.GetBoolean(0));
		Assert.Equal(expected.Integer, r.GetInt32(1));
		Assert.Equal(expected.Double, r.GetDouble(2));
		Assert.Equal(expected.Date, r.GetDateTime(3));
		Assert.Equal(expected.Text, r.GetString(4));
	}

	[Fact]
	public void BinaryBase64()
	{
		var data =
			new[] {
				 new {
					  Name = "A",
					  Value = new byte[] { 1, 2, 3, 4, 5 },
				}
			};

		var csv = GetCsv(data);
		Assert.Equal("Name,Value\nA,AQIDBAU=\n", csv);
	}

	[Fact]
	public void BinaryHex()
	{
		var data =
			new[] {
				 new {
					  Name = "A",
					  Value = new byte[] { 1, 2, 3, 4, 5, 15, 16 },
				}
			};

		var opt =
			new CsvDataWriterOptions
			{
				BinaryEncoding = BinaryEncoding.Hexadecimal,
				NewLine = "\n"
			};

		var csv = GetCsv(data.AsDataReader(), opt);
		Assert.Equal("Name,Value\nA,01020304050f10\n", csv);
	}

	[Fact]
	public void BinaryBase64Big()
	{
		BinaryBig(BinaryEncoding.Base64, 0x4000, null, false);
	}

	[Fact]
	public void BinaryHexBig()
	{
		BinaryBig(BinaryEncoding.Hexadecimal, 0x4000, null, false);
	}

	[Fact]
	public void BinaryBase64BigGrow()
	{
		BinaryBig(BinaryEncoding.Base64, 0x4000, 0x10000, true);
	}

	[Fact]
	public void BinaryHexBigGrow()
	{
		BinaryBig(BinaryEncoding.Hexadecimal, 0x4000, 0x10000, true);
	}

	static void BinaryBig(BinaryEncoding encoding, int bufferSize, int? maxBufferSize, bool succeed)
	{
		// select a size that will overlap the default buffersize when two fields are written
		var bytes = Enumerable.Range(0, bufferSize * 3 / 4).Select(i => (byte)i).ToArray();
		var data =
			new[] {
				 new {
					  Name = "A",
					  Value = bytes,
					  Value2 = bytes,
				}
			};

		var opt =
			new CsvDataWriterOptions
			{
				BufferSize = bufferSize,
				MaxBufferSize = maxBufferSize,
				BinaryEncoding = encoding,
				NewLine = "\n",
				Delimiter = '\t',
			};

		var writeFunc = () => GetCsv(data.AsDataReader(), opt);

		if (succeed)
		{
			writeFunc();
		}
		else
		{
			Assert.Throws<CsvRecordTooLargeException>(writeFunc);
			return;
		}

		var csv = GetCsv(data.AsDataReader(), opt);

		var readerOpts =
			new CsvDataReaderOptions
			{
				BufferSize = 0x10000,
				BinaryEncoding = encoding
			};
		var csvr = CsvDataReader.Create(new StringReader(csv), readerOpts);
		Assert.True(csvr.Read());
		Assert.Equal(bytes, csvr.GetBytes(1));
		Assert.Equal(bytes, csvr.GetBytes(2));
	}

	[Fact]
	public void WriteDate()
	{
		var data = new[]
			{
				new { Date = new DateTime(2021, 2, 6, 0, 0, 0, DateTimeKind.Local) },
				new { Date = new DateTime(2021, 2, 6, 1, 2, 3, DateTimeKind.Local) },
				new { Date = new DateTime(2021, 2, 6, 0, 0, 0, DateTimeKind.Utc) },
				new { Date = new DateTime(2021, 2, 6, 1, 2, 3, DateTimeKind.Utc) },
				new { Date = new DateTime(2021, 2, 6, 0, 0, 0, DateTimeKind.Unspecified) },
				new { Date = new DateTime(2021, 2, 6, 1, 2, 3, DateTimeKind.Unspecified) },
			};

		var csvStr = GetCsv(data);
		var csv = CsvDataReader.Create(new StringReader(csvStr));
		var idx = 0;
		while (csv.Read())
		{
			var expected = data[idx];
			var result = csv.GetDateTime(0);
			Assert.Equal(expected.Date.ToUniversalTime(), result.ToUniversalTime());
			idx++;
		}
	}

	[Theory]
	[InlineData("MM/dd/yyyy HH:mm:ss")]
	// format that contains a comma
	[InlineData("ddd',' MMM dd yyyy hh:mm:ss t")]
	// a format string that produces a string that is longer than the IsoDate.MaxLength
	[InlineData("'long prefix 'yyyy-MM-dd' 'HH:mm:ss.fffffffZ' long suffix'")]
	public void WriteDateTimeCustomFormat(string fmt)
	{
		var data = new[]
			{
				new { Date = new DateTime(2021, 2, 6, 0, 0, 0) },
				new { Date = new DateTime(2021, 2, 6, 1, 2, 3) },
			};
		var opts = new CsvDataWriterOptions { DateTimeFormat = fmt };
		var csvStr = GetCsv(data, opts);

		var ropts = new CsvDataReaderOptions { DateTimeFormat = fmt };
		var csv = CsvDataReader.Create(new StringReader(csvStr), ropts);
		var idx = 0;
		while (csv.Read())
		{
			var expected = data[idx];
			var result = csv.GetDateTime(0);
			Assert.Equal(expected.Date, result);
			idx++;
		}
	}

	[Fact]
	public void WriteDateTimeOffset()
	{
		var offset = TimeZoneInfo.Local.GetUtcOffset(new DateTime(2021, 2, 6));
		var offset1H = offset.Add(TimeSpan.FromHours(1));
		var data = new[]
			{
				new { Date = new DateTimeOffset(2021, 2, 6, 0, 0, 0, offset) },
				new { Date = new DateTimeOffset(2021, 2, 6, 1, 2, 3, offset ) },
				new { Date = new DateTimeOffset(2021, 2, 6, 0, 0, 0, TimeSpan.Zero ) },
				new { Date = new DateTimeOffset(2021, 2, 6, 1, 2, 3, TimeSpan.Zero ) },
				new { Date = new DateTimeOffset(2021, 2, 6, 0, 0, 0, offset1H ) },
				new { Date = new DateTimeOffset(2021, 2, 6, 1, 2, 3, offset1H ) },
			};

		var csvStr = GetCsv(data);
		var csv = CsvDataReader.Create(new StringReader(csvStr));
		var idx = 0;
		while (csv.Read())
		{
			var expected = data[idx];
			var result = csv.GetDateTimeOffset(0);
			Assert.Equal(expected.Date, result);
			idx++;
		}
	}

	[Fact]
	public void EscapedStyle1()
	{
		var opts = new CsvDataWriterOptions { Style = CsvStyle.Escaped, Escape = '\\', NewLine = "\n" };
		var sw = new StringWriter();
		var w = CsvDataWriter.Create(sw, opts);

		var data = new[]
			{
				new
				{
					Name = "Value with comma, and \r\n newline.",
					Value = 12,
				},
				new
				{
					Name = "#Comment",
					Value = 16,
				},
			};

		w.Write(data.AsDataReader());
		var str = sw.ToString();
		Assert.Equal("Name,Value\nValue with comma\\, and \\\r\n newline.,12\n\\#Comment,16\n", str);
	}


	[Fact]
	public void EscapedStyle2()
	{
		var opts = new CsvDataWriterOptions { Style = CsvStyle.Escaped, Escape = '\\', NewLine = "\n" };
		var sw = new StringWriter();
		var w = CsvDataWriter.Create(sw, opts);

		var data = new[]
			{
				new
				{
					Name = "Value with comma, and \r\n newline.",
					Value = 12,
				},
				new
				{
					Name = "#Comment",
					Value = 16,
				},
			};

		w.Write(data.AsDataReader());
		var str = sw.ToString();
		Assert.Equal("Name,Value\nValue with comma\\, and \\\r\n newline.,12\n\\#Comment,16\n", str);
	}

	[Fact]
	public void WriteQuote()
	{
		var data = new[]
			{
				new
				{
					Text = "Test, 1",
				},
				new
				{
					Text = "\"test2\"",
				},
			};

		var csv = GetCsv(data);
		Assert.Equal("Text\n\"Test, 1\"\n\"\"\"test2\"\"\"\n", csv);
	}

	[Fact]
	public void CultureCommaDecimalPoint()
	{
		var sw = new StringWriter();
		var csv = CsvDataWriter.Create(sw, new CsvDataWriterOptions { NewLine = "\n", Culture = ItalianCulture });

		var dr = new[]
		{
			new { Value = 12.34 },
		};

		csv.Write(dr.AsDataReader());
		var str = sw.ToString();
		Assert.Equal("Value\n\"12,34\"\n", str);
	}

	[Fact]
	public void CultureCommaDecimalPoint2()
	{
		var sw = new StringWriter();
		var csv = CsvDataWriter.Create(sw, new CsvDataWriterOptions { NewLine = "\n", Culture = ItalianCulture, Delimiter = ';' });

		var dr = new[]
		{
			new { Name= "A", Value = 12.34 },
		};

		csv.Write(dr.AsDataReader());
		var str = sw.ToString();
		Assert.Equal("Name;Value\nA;12,34\n", str);
	}

	[Fact]
	public void Comment1()
	{
		var sw = new StringWriter();
		var csv = CsvDataWriter.Create(sw, TestOptions);

		var dr = new[]
		{
			new { Name = "#1", Value = "#2" },
		};

		csv.Write(dr.AsDataReader());
		var str = sw.ToString();
		Assert.Equal("Name,Value\n\"#1\",\"#2\"\n", str);
	}

	[Fact]
	public void BufferSpanBugDateTime()
	{
		var date = new DateTime(2000, 1, 1);
		BufferSpanBug(i => date.AddDays(i), dr => dr.GetDateTime(1));
	}

	[Fact]
	public void BufferSpanBugInt32()
	{
		BufferSpanBug(i => i + 1000, dr => dr.GetInt32(1));
	}

	[Fact]
	public void BufferSpanBugInt64()
	{
		BufferSpanBug(i => i + 1000L, dr => dr.GetInt64(1));
	}

	[Fact]
	public void BufferSpanBugFloat()
	{
		BufferSpanBug(i => 1.5f * i, dr => dr.GetFloat(1));
	}

	[Fact]
	public void BufferSpanBugDouble()
	{
		BufferSpanBug(i => 1.125 * i, dr => dr.GetDouble(1));
	}

	[Fact]
	public void BufferSpanBugGuid()
	{
		BufferSpanBug(i => Guid.NewGuid(), dr => dr.GetGuid(1));
	}

	static void BufferSpanBug<T>(Func<int, T> allocator, Func<DbDataReader, T> selector)
	{
		// There was a bug where values that spanned buffers wouldn't be written at all
		const int RecordCount = 10000;
		var sw = new StringWriter();
		var csv = CsvDataWriter.Create(sw, TestOptions);

		var data =
			Enumerable
			.Range(0, RecordCount)
			.Select(i => new { Id = i, Value = allocator(i) })
			.ToArray();

		csv.Write(data.AsDataReader());
		var str = sw.ToString();
		var reader = new StringReader(str);
		var csvReader = CsvDataReader.Create(reader);

		int c = 0;
		while (csvReader.Read())
		{
			var i = csvReader.GetInt32(0);
			Assert.Equal(c, i);
			var d = selector(csvReader);
			Assert.Equal(data[i].Value, d);
			c++;
		}
		Assert.Equal(RecordCount, c);
	}


	[Fact]
	public void CsvWriteBatches()
	{
		using var tw = File.CreateText("output.csv");
		var data = "A,B,C\n1,2,3\n4,5,6\n";

		{
			var r = CsvDataReader.Create(new StringReader(data));
			var csvWriter = CsvDataWriter.Create(tw);
			csvWriter.Write(r);
		}

		{
			var r = CsvDataReader.Create(new StringReader(data));
			var csvWriter = CsvDataWriter.Create(tw, new CsvDataWriterOptions { WriteHeaders = false });
			csvWriter.Write(r);
		}
	}

	[Theory]
	[InlineData(CsvStringQuoting.Default, "Name\n1\n\n\n")]
	[InlineData(CsvStringQuoting.AlwaysQuoteEmpty, "Name\n1\n\"\"\n\n")]
	[InlineData(CsvStringQuoting.AlwaysQuoteNonEmpty, "\"Name\"\n\"1\"\n\n\n")]
	[InlineData(CsvStringQuoting.AlwaysQuote, "\"Name\"\n\"1\"\n\"\"\n\n")]
	public void QuoteStringsOptions(CsvStringQuoting mode, string result)
	{
		var data = new[]
			{
				new
				{
					Name = "1"
				},
				new
				{
					Name = ""
				},
				new
				{
					Name = (string)null,
				},
			};
		var reader = data.AsDataReader();
		var opts = new CsvDataWriterOptions { QuoteStrings = mode, NewLine = "\n" };
		var sw = new StringWriter();
		var writer = CsvDataWriter.Create(sw, opts);
		writer.Write(reader);
		var str = sw.ToString();
		Assert.Equal(result, str);
	}

#if !NET48
	
	// don't run this one on .NET 4.8, because the floating point formatting
	// is slightly different, and the .NET 6.0 coverage is sufficient.
	[Fact]
	public void TypeCoverageTest()
	{
		var dt = new DateTime(2023, 12, 31, 23, 59, 59, 999);
		var data = new[] {
			new
			{
				Byte = byte.MaxValue,
				Int16 = short.MaxValue,
				Int32 = int.MaxValue,
				Int64 = long.MaxValue,
				Single = float.MaxValue,
				Double = double.MaxValue,
				Decimal = decimal.MaxValue,
				DateTime = dt,
				DateTimeOffset = new DateTimeOffset(dt, TimeSpan.FromHours(-8)),
				Char = '\u2122'
			},
			new
			{
				Byte = byte.MinValue,
				Int16 = short.MinValue,
				Int32 = int.MinValue,
				Int64 = long.MinValue,
				Single = float.MinValue,
				Double = double.MinValue,
				Decimal = decimal.MinValue,
				DateTime = dt,
				DateTimeOffset = new DateTimeOffset(dt, TimeSpan.FromHours(-8)),
				Char = ','
			}
		};
		var sw = new StringWriter();
		var w = CsvDataWriter.Create(sw, TestOptions);
		w.Write(data.AsDataReader());
		var str = sw.ToString();

		var expected = "Byte,Int16,Int32,Int64,Single,Double,Decimal,DateTime,DateTimeOffset,Char\n255,32767,2147483647,9223372036854775807,3.4028235E+38,1.7976931348623157E+308,79228162514264337593543950335,2023-12-31T23:59:59.9990000,2023-12-31T23:59:59.9990000-08:00,\u2122\n0,-32768,-2147483648,-9223372036854775808,-3.4028235E+38,-1.7976931348623157E+308,-79228162514264337593543950335,2023-12-31T23:59:59.9990000,2023-12-31T23:59:59.9990000-08:00,\",\"\n";
		Assert.Equal(expected, str);
	}

#endif

	static DbDataReader GetTestReader()
	{
		var data = new[]
		{
			new {
				Id = 1,
				Name = "A",
				Value = 1.23,
				Date = new DateTime(2020, 12, 11, 0, 0, 0),
				FileSize = long.MinValue
			},
			new {
				Id = 2,
				Name = "B",
				Value = 3.45,
				Date = new DateTime(2021, 7, 13, 3, 4, 5),
				FileSize = long.MaxValue
			},
		};
		return data.AsDataReader();
	}

#if NET48
	const string TestResult = "Id,Name,Value,Date,FileSize\n1,A,1.23,2020-12-11T00:00:00.0000000,-9223372036854775808\n2,B,3.45,2021-07-13T03:04:05.0000000,9223372036854775807\n";
#else
	const string TestResult = "Id,Name,Value,Date,FileSize\n1,A,1.23,2020-12-11,-9223372036854775808\n2,B,3.45,2021-07-13T03:04:05,9223372036854775807\n";
#endif

	[Fact]
	public void WriteSchemaless()
	{
		var dr = GetTestReader();
		// reader doesn't implement GetColumnSchema
		// and returns null for GetSchemaTable
		var tr = new SchemalessDataReader(dr);
		var sw = new StringWriter();
		
		var csvw = CsvDataWriter.Create(sw, TestOptions);
		csvw.Write(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Fact]
	public void WriteSchemaTable()
	{
		var dr = GetTestReader();
		// reader only provides GetSchemaTable
		var tr = new SchemaTableDataReader(dr);
		var sw = new StringWriter();
		var csvw = CsvDataWriter.Create(sw, TestOptions);
		csvw.Write(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Fact]
	public void WriteNullFieldType()
	{
		var dr = GetTestReader();
		// reader provides no schema
		// and returns null for GetFieldType(int)
		var tr = new NullFieldTypeDataReader(dr);
		var sw = new StringWriter();
		var csvw = CsvDataWriter.Create(sw, TestOptions);
		csvw.Write(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Fact]
	public void WriteObjectFieldType()
	{
		var dr = GetTestReader();
		// reader provides no schema
		// and returns typeof(object) for GetFieldType(int)
		var tr = new ObjectFieldTypeDataReader(dr);
		var sw = new StringWriter();
		var csvw = CsvDataWriter.Create(sw, TestOptions);
		csvw.Write(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Fact]
	public async Task WriteSchemalessAsync()
	{
		var dr = GetTestReader();
		// reader doesn't implement GetColumnSchema
		// and returns null for GetSchemaTable
		var tr = new SchemalessDataReader(dr);
		var sw = new StringWriter();

		var csvw = CsvDataWriter.Create(sw, TestOptions);
		await csvw.WriteAsync(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Fact]
	public async Task WriteSchemaTableAsync()
	{
		var dr = GetTestReader();
		// reader only provides GetSchemaTable
		var tr = new SchemaTableDataReader(dr);
		var sw = new StringWriter();
		var csvw = CsvDataWriter.Create(sw, TestOptions);
		await csvw.WriteAsync(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Fact]
	public async Task WriteNullFieldTypeAsync()
	{
		var dr = GetTestReader();
		// reader provides no schema
		// and returns null for GetFieldType(int)
		var tr = new NullFieldTypeDataReader(dr);
		var sw = new StringWriter();
		var csvw = CsvDataWriter.Create(sw, TestOptions);
		await csvw.WriteAsync(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Fact]
	public async Task WriteObjectFieldTypeAsync()
	{
		var dr = GetTestReader();
		// reader provides no schema
		// and returns typeof(object) for GetFieldType(int)
		var tr = new ObjectFieldTypeDataReader(dr);
		var sw = new StringWriter();
		var csvw = CsvDataWriter.Create(sw, TestOptions);
		await csvw.WriteAsync(tr);
		Assert.Equal(TestResult, sw.ToString());
	}

	[Theory]
#if NET6_0_OR_GREATER
	[InlineData(null, "2022-01-02\n2022-11-12\n2022-11-12T13:14:15\n")]
#else
	[InlineData(null, "2022-01-02T00:00:00.0000000\n2022-11-12T00:00:00.0000000\n2022-11-12T13:14:15.0000000\n")]
#endif
	[InlineData("yyyyMMdd", "20220102\n20221112\n20221112\n")]
	public void WriteDateTimeFormat(string fmt, string expected)
	{
		var sw = new StringWriter();
		var opts = new CsvDataWriterOptions
		{
			WriteHeaders = false,
			NewLine = "\n",
			DateTimeFormat = fmt
		};

		var cw = CsvDataWriter.Create(sw, opts);
		var data = new[]
		{
			new { Date = new DateTime(2022, 1, 2) },
			new { Date = new DateTime(2022, 11, 12) },
			new { Date = new DateTime(2022, 11, 12, 13, 14, 15) },
		};
		cw.Write(data.AsDataReader());
		var result = sw.ToString();
		Assert.Equal(expected, result);
	}

#if NET6_0_OR_GREATER

	[Fact]
	public void WriteDateOnly()
	{
		var data = new[]
			{
				new
				{
					Name = "a",
					Value = new DateOnly(2022, 8, 3),
				},
				new
				{
					Name = "b",
					Value = new DateOnly(2011, 12, 13),
				},
			};

		var sw = new StringWriter();

		var opts = new CsvDataWriterOptions
		{
			DateOnlyFormat = "dd/MM/yyyy",
			NewLine = "\n"
		};
		var csvw = CsvDataWriter.Create(sw, opts);
		var reader = data.AsDataReader();
		csvw.Write(reader);

		var str = sw.ToString();
		Assert.Equal("Name,Value\na,03/08/2022\nb,13/12/2011\n", str);
	}

#endif
}
