using System;
using System.Linq;
using Xunit;

namespace Sylvan.Data
{
	public class ObjectDataReaderTests
	{
		[Fact]
		public void Test1()
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
	}
}
