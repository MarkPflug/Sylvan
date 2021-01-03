using System.Data.Common;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class DataBinderTests
	{
		class Record
		{
			public int UID { get; set; }
			public string Admin2 { get; set; }
			public string Province_State { get; set; }
			[ColumnSeries("{Date}")]
			public DateSeries<int> Values { get; set; }
		}

		const string SchemaSpec = "UID:int,Admin2,Province_State,{Date}>Values*:int";

		ICsvSchemaProvider schema;
		CsvDataReaderOptions opts;

		IDataBinder<Record> binder;

		public DataBinderTests()
		{
			var s = Schema.Parse(SchemaSpec).GetColumnSchema();
			this.schema = new CsvSchema(s);
			opts = new CsvDataReaderOptions { Schema = schema };

			var data = TestData.GetDataWithSchema();
			this.binder = DataBinder<Record>.Create(s, data.GetColumnSchema());
		}

		[Fact]
		public void Compiled()
		{
			var data = TestData.GetDataWithSchema();
			while (data.Read())
			{
				var f = new Record();
				binder.Bind(data, f);
				Assert.NotEqual(default, f.UID);
				Assert.NotEqual(default, f.Admin2);
				Assert.NotEqual(default, f.Province_State);
				Assert.NotNull(f.Values);
			}
		}
	}
}
