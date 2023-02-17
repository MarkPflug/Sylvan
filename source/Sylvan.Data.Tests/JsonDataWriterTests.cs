#if NET6_0_OR_GREATER
using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data;

public class JsonDataWriterTests
{
	[Fact]
	public void Sync()
	{
		var data =
			Enumerable
			.Range(1, 10)
			.Select(
				i =>
				new
				{
					Id = i,
					Name = "Test " + i,
					Date = new DateTime(2022, 1, 1).AddDays(i * 3.1),
					Measure = i * Math.PI,
					Value = 12.37m * i,
				}
			)
			.AsDataReader();

		var ms = new MemoryStream();
		var count = data.WriteJson(ms);
		Assert.Equal(10, count);
		ms.Seek(0, SeekOrigin.Begin);
		var r = new StreamReader(ms);
		var jsonStr = r.ReadToEnd();

	}

	[Fact]
	public async Task Async()
	{
		var data =
			Enumerable
			.Range(1, 10)
			.Select(
				i =>
				new
				{
					Id = i,
					Name = "Test " + i,
					Date = new DateTime(2022, 1, 1).AddDays(i * 3.1),
					Measure = i * Math.PI,
					Value = 12.37m * i,
				}
			)
			.AsDataReader();

		var ms = new MemoryStream();
		var count = await data.WriteJsonAsync(ms);
		Assert.Equal(10, count);
		ms.Seek(0, SeekOrigin.Begin);
		var r = new StreamReader(ms);
		var jsonStr = r.ReadToEnd();
	}

	static JsonDocument GetDocument(DbDataReader data)
	{
		var ms = new MemoryStream();
		data.WriteJson(ms);
		ms.Seek(0, SeekOrigin.Begin);
		var r = new StreamReader(ms);
		var jsonStr = r.ReadToEnd();

		 return JsonDocument.Parse(jsonStr);
	}

	[Fact]
	public void DateTimeLocal()
	{
		// Local times are adjusted to UTC and written with a trailing "Z"
		var date = new DateTime(2022, 12, 11, 10, 9, 8, DateTimeKind.Local);
		var utc = date.ToUniversalTime();

		var data = new[] { new { Id = 1, Date = date, } }
			.AsDataReader();
		var jsonDoc = GetDocument(data);
		
		var dateProp = jsonDoc.RootElement[0].GetProperty("Date");
		Assert.Equal(utc, dateProp.GetDateTime());
	}

	[Fact]
	public void DateTimeUtc()
	{
		// UTC times are written with a trailing "Z"
		var date = new DateTime(2022, 12, 11, 10, 9, 8, DateTimeKind.Utc);

		var data = new[] { new { Id = 1, Date = date, } }
			.AsDataReader();

		var jsonDoc = GetDocument(data);
		var dateProp = jsonDoc.RootElement[0].GetProperty("Date");
		Assert.Equal(date, dateProp.GetDateTime());
	}

	[Fact]
	public void DateTimeUnspecified()
	{
		// unspecified times are written without offset adjustment
		// and don't have a trailing "Z"
		var date = new DateTime(2022, 12, 11, 10, 9, 8, DateTimeKind.Unspecified);

		var data = new[] { new { Id = 1, Date = date, } }
			.AsDataReader();

		var jsonDoc = GetDocument(data);
		var dateProp = jsonDoc.RootElement[0].GetProperty("Date");
		Assert.Equal(date, dateProp.GetDateTime());
	}

	[Fact]
	public void DateTimeOffsetTest()
	{
		var dto = new DateTimeOffset(2022, 12, 11, 10, 9, 8, TimeSpan.FromHours(-8));

		var data = new[] { new { Id = 1, Date = dto, } }
			.AsDataReader();

		var jsonDoc = GetDocument(data);
		var dateProp = jsonDoc.RootElement[0].GetProperty("Date");
		Assert.Equal(dto, dateProp.GetDateTimeOffset());
	}

	[Fact]
	public void GuidTest()
	{
		var g = Guid.NewGuid();

		var data = new[] { new { Id = 1, Guid = g, } }
			.AsDataReader();

		var jsonDoc = GetDocument(data);
		var prop = jsonDoc.RootElement[0].GetProperty("Guid");
		Assert.Equal(g, prop.GetGuid());
	}

	[Fact]
	public void BinaryTest()
	{
		var bytes = Guid.NewGuid().ToByteArray();

		var data = new[] { new { Id = 1, Bytes = bytes, } }
			.AsDataReader();

		var jsonDoc = GetDocument(data);
		var prop = jsonDoc.RootElement[0].GetProperty("Bytes");
		Assert.Equal(bytes, prop.GetBytesFromBase64());
	}

	[Fact]
	public void Test()
	{

		var obj = new
		{
			Date = DateTimeOffset.Now
		};
		var j = JsonSerializer.Serialize(obj);

	}
}

#endif