using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data;

public class ObjectDataReaderTests
{
	[Fact]
	public void Sync()
	{
		var data = Enumerable
			.Range(1, 10)
			.Select(i =>
				new
				{
					Name = "Hi" + i,
					Id = i,
					Date = DateTime.Today.AddDays(i),
					Guid = Guid.NewGuid(),
					NullId = i % 2 == 1 ? i : (int?)null,
				}
			);

		var dr = ObjectDataReader.Create(data);
		int count = 0;
		while (dr.Read())
		{
			var name = dr.GetString(0);
			var id = dr.GetInt32(1);
			var date = dr.GetDateTime(2);
			var g = dr.GetGuid(3);
			if (!dr.IsDBNull(4))
			{
				dr.GetInt32(4);
			}

			count++;
		}
		Assert.Equal(10, count);
	}

	class Record
	{
		public string Name { get; set; }
		public int Id { get; set; }
		public DateTime Date { get; set; }
		public Guid Guid { get; set; }
		public int? NullId { get; set; }
	}

#if NET6_0_OR_GREATER

	static async IAsyncEnumerable<Record> GetRecordsAsync(int c)
	{
		for (int i = 0; i < c; i++)
		{
			await Task.Yield();
			yield return new Record
			{
				Name = "Hi" + i,
				Id = i,
				Date = DateTime.Today.AddDays(i),
				Guid = Guid.NewGuid(),
				NullId = i % 2 == 1 ? i : (int?)null,
			};
		}
	}

	[Fact]
	public async Task Async()
	{
		var dr = GetRecordsAsync(10).AsDataReader();
		int count = 0;
		while (await dr.ReadAsync())
		{
			var name = dr.GetString(0);
			var id = dr.GetInt32(1);
			var date = dr.GetDateTime(2);
			var g = dr.GetGuid(3);
			if (!dr.IsDBNull(4))
			{
				dr.GetInt32(4);
			}

			count++;
		}
		Assert.Equal(10, count);
	}

#endif

	public class UserRecord
	{
		[DataMember(Name = "User Name")]
		public string Name { get; set; }

		[DataMember(Name = "Date of Birth")]
		public DateTime DOB { get; set; }
	}

	[Fact]
	public void DataMemberName()
	{
		UserRecord[] records = new[]
		{
			new UserRecord { Name="Jim", DOB = new DateTime(1975, 3, 12) },
			new UserRecord { Name="Kim", DOB = new DateTime(1971, 7, 4) },
		};

		using var reader = records.AsDataReader();
		var sw = new StringWriter();
		using (var w = CsvDataWriter.Create(sw))
		{
			w.Write(reader);
		}
		var str = sw.ToString();
		var sr = new StringReader(str);
		var header = sr.ReadLine();
		Assert.Equal("User Name,Date of Birth", header);
	}
}
