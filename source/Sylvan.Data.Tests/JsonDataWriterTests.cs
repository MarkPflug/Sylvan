#if NET6_0_OR_GREATER
using System;
using System.IO;
using System.Linq;
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
}

#endif