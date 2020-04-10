using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvWriterTests
	{

		static string GetExpected()
		{
			return File.ReadAllText(@"Data\Writer_3_1_Expected.csv");
		}

#if NETCOREAPP3_1

		// these tests are a problem in net 461 because of changes to float formatting

		[Fact]
		public async Task Async()
		{
			var tw = new StringWriter();
			var items = TestData.GetTestObjects(3, 1);
			await using (var csv = new CsvWriter(tw))
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
	}
}
