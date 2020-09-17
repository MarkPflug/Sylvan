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

	enum Severity
	{
		Critical = 1,
		Error = 2,
		Warning = 3,
		Info = 4,
		Verbose = 5,
	}

	class EnumRecord
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public Severity Severity { get; set; }
	}

	public class DataBinderTests
	{
		static ReadOnlyCollection<DbColumn> BuildSchema()
		{
			var sb = new Schema.Builder();
			sb.AddColumn(null, DbType.Int32, false);
			sb.AddColumn(null, DbType.String, false);
			sb.AddColumn(null, DbType.DateTime, true);
			var schema = sb.Build();
			return schema.GetColumnSchema();
		}

		[Fact]
		public void Test1()
		{
			var schema = BuildSchema();
			var binder = new ReflectionDataBinder<MyDataRecord>(schema);

			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			while (data.Read())
			{
				var item = binder.Bind(data);
			}
		}

		[Fact]
		public void Test2()
		{
			var schema = BuildSchema();
			var binder = new CompiledDataBinder<MyDataRecord>(schema);

			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			while (data.Read())
			{
				var item = binder.Bind(data);
			}
		}

		[Fact]
		public void TestEnumByValue()
		{
			var schema = Schema.TryParse(":int,:string,:int").GetColumnSchema();
			

			var csvData = "Id,Name,Severity\r\n1,Olive,3";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);

			var binder = new CompiledDataBinder<EnumRecord>(data.GetColumnSchema());

			while (data.Read())
			{
				var item = binder.Bind(data);
			}
		}

		[Fact]
		public void TestByName()
		{
			var schema = Schema.TryParse(":int,:string,:string").GetColumnSchema();

			var csvData = "Id,Name,Severity\r\n1,Olive,Warning";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);

			var binder = new CompiledDataBinder<EnumRecord>(data.GetColumnSchema());

			while (data.Read())
			{
				var item = binder.Bind(data);
			}
		}
	}
}
