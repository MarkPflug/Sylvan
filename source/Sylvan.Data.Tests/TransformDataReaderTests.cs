using Sylvan.Data.Csv;
using System.Data.Common;
using System.IO;
using Xunit;

namespace Sylvan.Data
{
	public class TransformDataReaderTests
	{
		[Fact]
		public void TransformDataReaderTest1()
		{
			var data = "a,b,c\n1,2,3\n,5,6\n7,8,9\n";
			DbDataReader reader = CsvDataReader.Create(new StringReader(data), new CsvDataReaderOptions { Schema = CsvSchema.Nullable });

			reader = reader.Select("a", "c").Where(dr => !dr.IsDBNull(0));

			while (reader.Read())
			{
				var a = reader.GetString(0);
				var b = reader.GetString(1);
			}
		}
	}
}
