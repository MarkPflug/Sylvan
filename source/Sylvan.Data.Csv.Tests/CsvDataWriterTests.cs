using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvDataWriterTests
	{
		[Fact]
		public async Task Simple()
		{
			var dr = TestData.GetData();

			var sw = new StringWriter();
			var csv = new CsvDataWriter(sw);
			await csv.WriteAsync(dr);
		}

		[Fact]
		public void DataTable()
		{
			var dr = TestData.GetData();
			var dt = new DataTable();
			dt.Load(dr);
			Assert.Equal(3253, dt.Rows.Count);
			Assert.Equal(85, dt.Columns.Count);
		}

		[Fact]
		public async Task Schema()
		{
			var dr = TestData.GetTypedData();
			var sw = new StringWriter();
			var csv = new CsvDataWriter(sw);
			await csv.WriteAsync(dr);
		}

		[Fact]
		public async Task Binary()
		{
			var dr = TestData.GetBinaryData();
			var sw = new StringWriter();
			using var csv = new CsvDataWriter(sw);
			await csv.WriteAsync(dr);
			var str = sw.ToString();
		}

		//[Fact]
		//public void WriteDate()
		//{
		//	var sw = new StringWriter();
		//	using (var csv = new CsvWriter(sw))
		//	{
		//		csv.WriteField(new DateTime(2020, 8, 7, 7, 45, 22));
		//	}
		//	var str = sw.ToString();
		//	Assert.Equal("2020-08-07T07:45:22", str);
		//}

		//[Fact]
		//public void WriteDateCustom()
		//{
		//	var sw = new StringWriter();
		//	using (var csv = new CsvWriter(sw, new CsvDataWriterOptions { DateFormat = "yyyyMMdd" }))
		//	{
		//		csv.WriteField(new DateTime(2020, 8, 7, 7, 45, 22));
		//	}
		//	var str = sw.ToString();
		//	Assert.Equal("20200807", str);
		//}

		class TypedObject
		{
			public int Id { get; set; }
		}


		[Fact]
		public void WriteTypes()
		{
			var data = TestData.GetTypedData();
			var tw = new StringWriter();
			using var csv = new CsvDataWriter(tw);
			csv.Write(data);
			var str = tw.ToString();
		}
	}
}
