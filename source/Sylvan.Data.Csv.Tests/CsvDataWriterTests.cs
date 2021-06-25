using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvDataWriterTests
	{
		// use \n for newlines to make assertions easier
		static CsvDataWriterOptions TestOptions = new CsvDataWriterOptions { NewLine = "\n" };

		// a culture that uses ',' for numeric decimal separator
		static CultureInfo ItalianCulture = CultureInfo.GetCultureInfoByIetfLanguageTag("it-IT");

		static string GetCsv<T>(IEnumerable<T> data) where T : class
		{
			var dr = data.AsDataReader();
			return GetCsv(dr);
		}

		static string GetCsv(DbDataReader dr)
		{
			var sw = new StringWriter();
			var csv = CsvDataWriter.Create(sw, TestOptions);
			csv.Write(dr);
			return sw.ToString();
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
			var expected = "Boolean,Integer,Double,Date,Text\nTrue,2147483647,15.25,2020-01-01T00:00:00,Abcd\n";
			Assert.Equal(expected, csv);
		}

		[Fact]
		public void Binary()
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
		public void WriteDate()
		{
			var data = new[]
				{
					new 
					{
						Name = "Date1", 
						Date = new DateTime(2021, 2, 6),
					},
					new 
					{
						Name = "Date2", 
						Date = new DateTime(2021, 2, 7),
					},
				};

			var csv = GetCsv(data);
			Assert.Equal("Name,Date\nDate1,2021-02-06T00:00:00\nDate2,2021-02-07T00:00:00\n", csv);
		}

		[Fact]
		public void UnquotedStyle()
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
			Assert.Equal("Name,Value\n\"#1\",#2\n", str);
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

		void BufferSpanBug<T>(Func<int, T> allocator, Func<DbDataReader,T> selector)
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
	}
}
