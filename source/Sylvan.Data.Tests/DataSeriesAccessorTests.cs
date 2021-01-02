using Sylvan.Data.Csv;
using System;
using System.IO;
using System.Linq;
using Xunit;

namespace Sylvan.Data
{
	public class DataSeriesAccessorTests
	{
		[Fact]
		public void Test1()
		{
			var data = "Id,Name,2020-01-01,2020-01-02,2020-01-03\r\n1,Test,5,6,7";
			var spec = "Id:int,Name,{Date}>Values*:int";
			var schema = new CsvSchema(SchemaSerializer.Simple.Read(spec).GetColumnSchema());
			var opts = new CsvDataReaderOptions { Schema = schema };
			var csv = CsvDataReader.Create(new StringReader(data), opts);


			var cols =
				csv.GetColumnSchema()
				.Where(c => DateTime.TryParse(c.ColumnName, out _))
				.Select(c => new DataSeriesColumn<DateTime>(c.ColumnName, DateTime.Parse(c.ColumnName), c.ColumnOrdinal.Value))
				.ToArray();

			var acc = new DataSeriesAccessor<DateTime, int>(cols);
			while (csv.Read())
			{
				var result = acc.GetSeries(csv).ToArray();
			}
		}
	}
}
