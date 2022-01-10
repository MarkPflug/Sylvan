using Sylvan.Data.Csv;
using System;
using System.Data.Common;
using System.IO;
using Xunit;
namespace Sylvan.Data
{
	public class ExtendedDataReaderTests
	{
		[Fact]
		public void Test1()
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
	}
}
