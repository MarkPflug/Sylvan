using System.Linq;
using Xunit;

namespace Sylvan.Data;

public class SkipTakeDataReaderTests
{
	[Fact]
	public void TakeTest()
	{
		var seq = Enumerable.Range(0, 100).Select(i => new { Name = "test" + i, Value = i });
		var reader = seq.AsDataReader();
		reader = reader.Take(10);

		for(int i = 0; i < 10; i++)
		{
			Assert.True(reader.Read());
		}
		Assert.False(reader.Read());
	}

	[Fact]
	public void SkipTest()
	{
		var seq = Enumerable.Range(0, 100).Select(i => new { Name = "test" + i, Value = i });
		var reader = seq.AsDataReader();
		reader = reader.Skip(60);

		for (int i = 0; i < 40; i++)
		{
			Assert.True(reader.Read());
		}
		Assert.False(reader.Read());
	}

	[Fact]
	public void SkipTakeTest()
	{
		var seq = Enumerable.Range(0, 100).Select(i => new { Name = "test" + i, Value = i });
		var reader = seq.AsDataReader();
		reader = reader.Skip(60).Take(20);

		for (int i = 0; i < 20; i++)
		{
			Assert.True(reader.Read());
			Assert.Equal(60 + i, reader.GetInt32(1));
		}
		Assert.False(reader.Read());
	}

	[Fact]
	public void TakeWhileTest()
	{
		var seq = Enumerable.Range(0, 100).Select(i => new { Name = "test" + i, Value = i });
		var reader = seq.AsDataReader();
		
		reader = reader.TakeWhile(dr => dr.GetInt32(1) < 50);

		for (int i = 0; i < 50; i++)
		{
			Assert.True(reader.Read());
			Assert.Equal(i, reader.GetInt32(1));
		}
		Assert.False(reader.Read());
	}
}
