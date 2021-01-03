﻿using Sylvan.Data.Csv;
using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
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

	class NumericNullRecord
	{
		public string Name { get; set; }
		public double? Value { get; set; }
	}

	class SeriesRecord
	{
		public int Id { get; private set; }
		public string Name { get; private set; }
		[ColumnSeries("{Integer}")]
		public Series<int> Values { get; private set; }
	}

	class SeriesDateRecord
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public DateSeries<int> Values { get; set; }
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
			var schema =
				new Schema.Builder()
				.Add<int>()
				.Add<string>()
				.Add<DateTime?>()
				.Build();
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
				var item = binder.GetRecord(data);
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
				var item = binder.GetRecord(data);
			}
		}

		[Fact]
		public void StringTest()
		{
			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			DbDataReader data = CsvDataReader.Create(tr);

			var binder = new CompiledDataBinder<MyDataRecord>(data.GetColumnSchema());

			while (data.Read())
			{
				var item = binder.GetRecord(data);
			}
		}

		[Fact]
		public void NumericNullTest()
		{
			var tr = new StringReader("Name,Value\nA,12.3\nB,\n");
			var dr = CsvDataReader.Create(tr);
			var binder = new CompiledDataBinder<NumericNullRecord>(dr.GetColumnSchema());

			while (dr.Read())
			{
				var item = binder.GetRecord(dr);
			}
		}

		[Fact]
		public void NumericNullManualTest()
		{
			var tr = new StringReader("Name,Value\nA,12.3\nB,\n");
			var dr = CsvDataReader.Create(tr);

			while (dr.Read())
			{
				var item = new NumericNullRecord();
				string tempStr;

				// This is essentially the block of code
				// that CompiledDataBinder should generate.

				if (!dr.IsDBNull(0))
				{
					item.Name = dr.GetString(0);
				}
				else
				{
					//optionally emit this.
					item.Name = default!;
				}
				if (!dr.IsDBNull(1))
				{
					tempStr = dr.GetString(1);
					if (!string.IsNullOrWhiteSpace(tempStr))
					{
						item.Value = double.Parse(tempStr);
					}
				}
				else
				{
					//optionally emit this.
					item.Value = default;
				}
			}
		}

		[Fact]
		public void TestEnumByValue()
		{
			var schema = Schema.Parse(":int,:string,:int").GetColumnSchema();

			var csvData = "Id,Name,Severity\r\n1,Olive,3";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);

			var binder = new CompiledDataBinder<EnumRecord>(data.GetColumnSchema());

			while (data.Read())
			{
				var item = binder.GetRecord(data);
			}
		}

		[Fact]
		public void TestEnumId()
		{
			var schema = Schema.Parse(":int,:string,:int").GetColumnSchema();

			var csvData = "Id,Name,Severity\r\n1,Olive,3\r\n2,Brown,2";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);

			var binder = new CompiledDataBinder<EnumRecord>(data.GetColumnSchema());

			Assert.True(data.Read());

			var item = binder.GetRecord(data);
			Assert.Equal(Severity.Warning, item.Severity);
			Assert.True(data.Read());
			item = binder.GetRecord(data);
			Assert.Equal(Severity.Error, item.Severity);
		}

		[Fact]
		public void TestEnumParse()
		{
			var schema = Schema.Parse(":boolean,:string,:string").GetColumnSchema();

			var csvData = "Id,Name,Severity\r\n1,Olive,Warning\r\n2,Brown,Error";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);

			var binder = new CompiledDataBinder<EnumRecord>(data.GetColumnSchema());

			Assert.True(data.Read());

			var item = binder.GetRecord(data);
			Assert.Equal(Severity.Warning, item.Severity);
			Assert.True(data.Read());
			item = binder.GetRecord(data);
			Assert.Equal(Severity.Error, item.Severity);
		}

		[Fact]
		public void SeriesInt()
		{
			var schema = Schema.Parse("Id:int,Name,{Integer}>Values*:int");
			var cols = schema.GetColumnSchema();
			var binderFactory = DataBinder<SeriesRecord>.CreateFactory(cols);

			var csvData = "Id,Name,1,2,3\n1,Test,7,8,9\n2,abc,11,12,13\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(cols) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			var binder = binderFactory.CreateBinder(data.GetColumnSchema());

			Assert.True(data.Read());
			var item = binder.GetRecord(data);
			Assert.Equal(new[] { 7, 8, 9 }, item.Values.Select(p => p.Value));
			Assert.True(data.Read());
			item = binder.GetRecord(data);
			Assert.Equal(new[] { 11, 12, 13 }, item.Values.Select(p => p.Value));
		}

		[Fact]
		public void SeriesDate()
		{
			var schemaSpec = "Id:int,Name,{Date}>Values*:int";
			var schema = Schema.Parse(schemaSpec);
			var cols = schema.GetColumnSchema();

			var binderFactory = DataBinder<SeriesDateRecord>.CreateFactory(cols);

			var csvData = "Id,Name,2020-09-19,2020-09-20,2020-09-21,2020-09-22\n1,Test,7,8,9,10\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			DbDataReader data = CsvDataReader.Create(tr, opts);
			var binder = binderFactory.CreateBinder(data.GetColumnSchema());

			while (data.Read())
			{
				var item = binder.GetRecord(data);
			}
		}

		[Fact]
		public void Manual()
		{
			var schema = Schema.Parse("Id:int,Name,{Date}>Values*:int").GetColumnSchema();

			var csvData = "Id,Name,2020-09-19,2020-09-20,2020-09-21\n1,Test,7,8,9\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);
			var binder = new ManualBinder(data.GetColumnSchema());

			data.Read();
			var item = binder.GetRecord(data);
			Assert.Equal(1, item.Id);
			Assert.Equal("Test", item.Name);
			Assert.Equal(new[] { 7, 8, 9 }, item.Values.Select(p => p.Value));
		}

		sealed class ManualBinder : IDataBinder<SeriesDateRecord>
		{
			readonly DataSeriesAccessor<DateTime, int> series0;
			int idIdx;
			int nameIdx;

			public ManualBinder(ReadOnlyCollection<DbColumn> schema)
			{
				idIdx = schema.Single(c => c.ColumnName == "Id").ColumnOrdinal.Value;
				nameIdx = schema.Single(c => c.ColumnName == "Name").ColumnOrdinal.Value;
				var seriesCols =
					schema
					.Where(c => DateTime.TryParse(c.ColumnName, out _))
					.Select(c => new DataSeriesColumn<DateTime>(c.ColumnName, DateTime.Parse(c.ColumnName), c.ColumnOrdinal.Value));
				this.series0 = new DataSeriesAccessor<DateTime, int>(seriesCols);
			}

			public void Bind(IDataRecord record, SeriesDateRecord item)
			{
				item.Id = record.GetInt32(idIdx);
				item.Name = record.GetString(nameIdx);
				item.Values = new DateSeries<int>(this.series0.Minimum, this.series0.GetValues(record));
			}
		}

		[Fact]
		public void Experiment()
		{
			Action<SeriesDateRecord, string> setter = (r, v) => r.Name = v;
			Func<int> getter = () => 1;

			Func<int, string> converter = i => i.ToString();

			var binder = BuildBinder(getter, converter, setter);
		}

		static Action<T> BuildBinder<T, TS, TD>(Func<TS> getter, Func<TS, TD> converter, Action<T, TD> setter)
		{
			return (T a) => setter(a, converter(getter()));
		}
	}
}
