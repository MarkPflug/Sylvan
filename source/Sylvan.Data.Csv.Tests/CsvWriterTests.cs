using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvWriterTests
	{

		static string GetExpected()
		{
			return File.ReadAllText(@"Data\WriterExpected.csv");
		}

		// these tests are a problem in net 461 because of changes to float formatting

#if NETCOREAPP3_1
		[Fact]
		public async Task Async()
		{
			var tw = new StringWriter();
			var items = TestData.GetTestObjects(3, 1);
			using (var csv = new CsvWriter(tw))
			{
				await csv.WriteFieldAsync("Id");
				await csv.WriteFieldAsync("Name");
				await csv.WriteFieldAsync("Date");
				await csv.WriteFieldAsync("IsActive");
				await csv.WriteFieldAsync("Value");
				await csv.EndRecordAsync();
				foreach (var item in items)
				{
					await csv.WriteFieldAsync(item.Id);
					await csv.WriteFieldAsync(item.Name);
					await csv.WriteFieldAsync(item.Date);
					await csv.WriteFieldAsync(item.IsActive);
					await csv.WriteFieldAsync(item.DataSet[0]);
					await csv.EndRecordAsync();
				}
			}

			var expected = GetExpected();
			Assert.Equal(expected, tw.ToString());
		}

		[Fact]
		public void Sync()
		{
			var tw = new StringWriter();
			var items = TestData.GetTestObjects(3, 1);
			using (var csv = new CsvWriter(tw))
			{
				csv.WriteField("Id");
				csv.WriteField("Name");
				csv.WriteField("Date");
				csv.WriteField("IsActive");
				csv.WriteField("Value");
				csv.EndRecord();
				foreach (var item in items)
				{
					csv.WriteField(item.Id);
					csv.WriteField(item.Name);
					csv.WriteField(item.Date);
					csv.WriteField(item.IsActive);
					csv.WriteField(item.DataSet[0]);
					csv.EndRecord();
				}

			}
			var expected = GetExpected();
			Assert.Equal(expected, tw.ToString());
		}

#endif

		[Fact]
		public void WriteQuote()
		{
			var tw = new StringWriter();
			using (var csv = new CsvWriter(tw))
			{
				csv.WriteField("Value with \"quote");
				csv.EndRecord();
			}
			var str = tw.ToString();
			Assert.Equal("\"Value with \"\"quote\"" + Environment.NewLine, str);
		}

		[Fact]
		public void WriteBinary()
		{
			var tw = new StringWriter();
			using (var csv = new CsvWriter(tw))
			{
				csv.WriteField(Encoding.ASCII.GetBytes("Hello, csv!"));
				csv.WriteField(Guid.Empty);
				csv.EndRecord();
			}
			var str = tw.ToString();
			Assert.Equal("SGVsbG8sIGNzdiE=,00000000-0000-0000-0000-000000000000\r\n", str);
		}

		const string DataResult = "True,AQIDBAU=,06/12/2020 12:15:15,1234.5,5432.25,00000000-0000-0000-0000-000000000000,9876,\"Value, with comma\",Value no comma\r\n";

		[Fact]
		public void WriteData()
		{
			var sw = new StringWriter();
			using var csv = new CsvWriter(sw);
			csv.WriteField(true);
			csv.WriteField(new byte[] { 1, 2, 3, 4, 5 });
			csv.WriteField(new DateTime(2020, 06, 12, 12, 15, 15, DateTimeKind.Utc));
			csv.WriteField(1234.5);
			csv.WriteField(5432.25f);
			csv.WriteField(Guid.Empty);
			csv.WriteField(9876);
			csv.WriteField("Value, with comma");
			csv.WriteField("Value no comma");
			csv.EndRecord();
			csv.Flush();
			var str = sw.ToString();
			Assert.Equal(DataResult, str);
		}
		
		[Fact]
		public async Task WriteDataAsync()
		{
			var sw = new StringWriter();
			using var csv = new CsvWriter(sw);
			await csv.WriteFieldAsync(true);
			await csv.WriteFieldAsync(new byte[] { 1, 2, 3, 4, 5 });
			await csv.WriteFieldAsync(new DateTime(2020, 06, 12, 12, 15, 15, DateTimeKind.Utc));
			await csv.WriteFieldAsync(1234.5);
			await csv.WriteFieldAsync(5432.25f);
			await csv.WriteFieldAsync(Guid.Empty);
			await csv.WriteFieldAsync(9876);
			await csv.WriteFieldAsync("Value, with comma");
			await csv.WriteFieldAsync("Value no comma");
			await csv.EndRecordAsync();
			await csv.FlushAsync();
			var str = sw.ToString();
			Assert.Equal(DataResult, str);
		}

		static CultureInfo GetCustomCulture()
		{
			var custom = (CultureInfo)CultureInfo.InvariantCulture.Clone();
			custom.NumberFormat.NumberDecimalSeparator = ",";
			custom.NumberFormat.NumberGroupSeparator = ".";
			return custom;
		}

		[Fact]
		public void CultureTest()
		{
			var c = GetCustomCulture();
			var str = 1234.5.ToString("#,#.#######", c);
		}
	}

	class TestCultureInfo : CultureInfo
	{
		public TestCultureInfo() : base(CurrentCulture.LCID)
		{

		}

		public override NumberFormatInfo NumberFormat { 
			get => base.NumberFormat; 
			set => base.NumberFormat = value; 
		}
	}
}
