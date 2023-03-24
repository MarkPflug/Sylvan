using Sylvan.Data.Csv;
using System;
using System.IO;
using Xunit;

namespace Sylvan.Data;

public class DynamicDataReaderTests
{
	[Fact]
	public void Test1()
	{
		var data = "a,b,c\n1,2022-11-12,12.3\n";
		var csv = CsvDataReader.Create(new StringReader(data));
		var dr = new DynamicDataReader(csv);

		Assert.True(dr.Read());

		Assert.Equal(typeof(object), dr.GetFieldType(0));
		Assert.Equal(typeof(object), dr.GetFieldType(1));
		Assert.Equal(typeof(object), dr.GetFieldType(2));

		Assert.Equal(1, dr.GetValue(0));
		Assert.Equal(new DateTime(2022, 11, 12), dr.GetValue(1));
		Assert.Equal(12.3m, dr.GetValue(2));
	}
}
