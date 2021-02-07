using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvDataWriterTests
	{
		// use \n for newlines to make assertions easier
		static CsvDataWriterOptions TestOptions = new CsvDataWriterOptions { NewLine = "\n" };

		// a culture that uses ',' for numeric decimal separator
		static CultureInfo ItalianCulture = CultureInfo.GetCultureInfoByIetfLanguageTag("it-IT");

		static string GetCsv<T>(IEnumerable<T> data)
		{
			var dr = data.AsDataReader();
			return GetCsv(dr);
		}

		static string GetCsv(DbDataReader dr)
		{
			var sw = new StringWriter();
			var csv = new CsvDataWriter(sw, TestOptions);
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
			var csv = new CsvDataWriter(sw, new CsvDataWriterOptions { NewLine = "\n", Culture = ItalianCulture });

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
			var csv = new CsvDataWriter(sw, new CsvDataWriterOptions { NewLine = "\n", Culture = ItalianCulture, Delimiter = ';' });

			var dr = new[]
			{
				new { Name= "A", Value = 12.34 },
			};

			csv.Write(dr.AsDataReader());
			var str = sw.ToString();
			Assert.Equal("Name;Value\nA;12,34\n", str);
		}
	}
}
