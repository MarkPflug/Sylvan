using Sylvan.Data.Csv;
using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using Xunit;

namespace Sylvan.Data
{

	class MyDataRecord
	{
		[ColumnOrdinal(0)]
		public int Id { get; private set; }
		public string Name { get; private set; }
		public DateTime? Date { get; private set; }
	}

	public class DataBinderTests
	{
		static readonly Lazy<ReadOnlyCollection<DbColumn>> Schema = new Lazy<ReadOnlyCollection<DbColumn>>(() => BuildSchema());

		static ReadOnlyCollection<DbColumn> BuildSchema()
		{
			var sb = new Schema.Builder();
			sb.AddColumn("Id", DbType.Int32, false);
			sb.AddColumn("Name", DbType.String, false);
			sb.AddColumn("Date", DbType.DateTime, true);
			var schema = sb.Build();
			return schema.GetColumnSchema();
		}

		static void Bind()
		{

		}

		[Fact]
		public void Test1()
		{
			var schema = BuildSchema();
			var binder = new DataBinder<MyDataRecord>(schema);

			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			while (data.Read())
			{
				var item = binder.Bind(data);
			}
		}
	}
}
