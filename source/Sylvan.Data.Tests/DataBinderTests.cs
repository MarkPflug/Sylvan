using Sylvan.Data.Csv;
using System;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using Xunit;

#if NET6_0_OR_GREATER
using DateType = System.DateOnly;
#else
using DateType = System.DateTime;
#endif

namespace Sylvan.Data
{
	public class DataBinderTests
	{
		static ReadOnlyCollection<DbColumn> BuildSchema()
		{
			var schema =
				new Schema.Builder()
				.Add<int>("Id")
				.Add<string>("Name")
				.Add<DateTime?>("Date")
				.Build();
			return schema.GetColumnSchema();
		}

		//[Fact]
		//public void Test1()
		//{
		//	var schema = BuildSchema();
		//	var binder = new ReflectionDataBinder<MyDataRecord>(schema);

		//	var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
		//	var tr = new StringReader(csvData);
		//	var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
		//	DbDataReader data = CsvDataReader.Create(tr, opts);

		//	while (data.Read())
		//	{
		//		var item = binder.GetRecord(data);
		//	}
		//}

		[Theory]
		[InlineData("Account Number,Day Of Week\n1,2\n")]
		[InlineData("Account_Number,day_of_week\n1,2\n")]
		[InlineData("account-number,day-of-week\n1,2\n")]
		[InlineData("accountnumber,dayofweek\n1,2\n")]
		[InlineData("ACCOUNTNUMBER,DAYOFWEEK\n1,2\n")]
		public void BinderHeaderMapping(string csvData)
		{
			var csv = CsvDataReader.Create(new StringReader(csvData));

			foreach (var record in csv.GetRecords<Record>())
			{
				Assert.Equal(1, record.AccountNumber);
				Assert.Equal(2, record.DayOfWeek);
			}
		}

		[Fact]
		public void IngoreDataMemberTest()
		{
			var csv = CsvDataReader.Create(new StringReader("AccountNumber,DayOfWeek,Name\n1,2,Test\n"));

			foreach (var record in csv.GetRecords<NotMappedRecord>())
			{
				Assert.Equal(1, record.AccountNumber);
				Assert.Equal(2, record.DayOfWeek);
				// Even though the source data has this column,
				// it doesn't get mapped, because it is marked with IgnoreDataMember
				Assert.Null(record.Name);
			}
		}

		[Fact]
		public void Test2()
		{
			var schema = BuildSchema();
			var binder = new CompiledDataBinder<MyDataRecord>(DataBinderOptions.Default, schema);

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
		public void RecordClass()
		{
			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			var data = CsvDataReader.Create(tr);

			foreach (var record in data.GetRecords<MyRecordClass>())
			{
				Assert.Equal(1, record.Id);
				Assert.Equal("Test", record.Name);
				Assert.Equal(new DateTime(2020, 8, 12), record.Date);
			}
		}

		[Fact]
		public void StringTest()
		{
			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			DbDataReader data = CsvDataReader.Create(tr);

			var binder = new CompiledDataBinder<MyDataRecord>(DataBinderOptions.Default, data.GetColumnSchema());

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
			var binder = DataBinder.Create<NumericNullRecord>(dr);

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

			var binder = new CompiledDataBinder<EnumRecord>(DataBinderOptions.Default, data.GetColumnSchema());

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

			var binder = new CompiledDataBinder<EnumRecord>(DataBinderOptions.Default, data.GetColumnSchema());

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

			var binder = new CompiledDataBinder<EnumRecord>(DataBinderOptions.Default, data.GetColumnSchema());

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

			var csvData = "Id,Name,1,2,3\n1,Test,7,8,9\n2,abc,11,12,13\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(cols) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			var binder = DataBinder.Create<SeriesRecord>(data, schema);

			Assert.True(data.Read());
			var item = binder.GetRecord(data);
			Assert.Equal(new[] { 7, 8, 9 }, item.Values.Select(p => p.Value));
			Assert.True(data.Read());
			item = binder.GetRecord(data);
			Assert.Equal(new[] { 11, 12, 13 }, item.Values.Select(p => p.Value));
		}

		[Fact]
		public void SeriesRangeAccessor()
		{
			var schema = Schema.Parse("Id:int,Name,{Integer}>Values*:int");
			var cols = schema.GetColumnSchema();

			var csvData = "Id,Name,1,2,3\n1,Test,7,8,9\n2,abc,11,12,13\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(cols) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			var binder = DataBinder.Create<SeriesRecord>(data, schema);
			var range = binder.GetSeriesRange<int>("Values");

			Assert.Equal(1, range.Minimum);
			Assert.Equal(3, range.Maximum);
		}

		[Fact]
		public void SeriesUnnamedRangeAccessor()
		{
			var schema = Schema.Parse("Id:int,Name,{Integer}>*:int");
			var cols = schema.GetColumnSchema();

			var csvData = "Id,Name,1,2,3\n1,Test,7,8,9\n2,abc,11,12,13\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(cols) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			var binder = DataBinder.Create<SeriesRecord>(data, schema);
			var range = binder.GetSeriesRange<int>("");

			Assert.Equal(1, range.Minimum);
			Assert.Equal(3, range.Maximum);
		}

		[Fact]
		public void SeriesString()
		{
			var schema = Schema.Parse("Id:int,Name,{string}>Values*:int");
			var cols = schema.GetColumnSchema();

			var csvData = "Id,Name,1,2,3\n1,Test,7,8,9\n2,abc,11,12,13\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(cols) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			var binder = DataBinder.Create<SeriesStringRecord>(data, schema);

			Assert.True(data.Read());
			var item = binder.GetRecord(data);
			Assert.Equal(new[] { 7, 8, 9 }, item.Values.Values);
			Assert.True(data.Read());
			item = binder.GetRecord(data);
			Assert.Equal(new[] { 11, 12, 13 }, item.Values.Values);
		}

		[Fact]
		public void SeriesDate()
		{
			var schemaSpec = "Id:int,Name,{Date}>Values*:int";
			var schema = Schema.Parse(schemaSpec);
			var cols = schema.GetColumnSchema();

			var csvData = "Id,Name,2020-09-19,2020-09-20,2020-09-21,2020-09-22\n1,Test,7,8,9,10\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);
			var binder = DataBinder.Create<SeriesDateRecord>(data, schema);

			while (data.Read())
			{
				var item = binder.GetRecord(data);
			}
		}

		class PopulationRecord
		{
			public string State { get; set; }
			public string County { get; set; }
			public Series<DateType, int> Values { get; set; }
		}

		[Fact]
		public void SimpleSeries()
		{
			var schemaSpec = "State,County,{Date}>Values*:int";
			var schema = Schema.Parse(schemaSpec);
			var cols = schema.GetColumnSchema();

			var csvData = "State,County,2020-03-01,2020-03-02,2020-03-03,2020-03-04\nOR,Washington,0,0,0,1\nOR,Multnomah,0,1,1,2\nOR,Linn,0,0,0,0\nOR,Deschutes,0,0,0,0";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			var data = CsvDataReader.Create(tr, opts);
			var binder = DataBinder.Create<PopulationRecord>(data, schema);

			while (data.Read())
			{
				var item = binder.GetRecord(data);
				Assert.NotNull(item.Values);
				Assert.Equal(4, item.Values.Keys.Count);
			}
		}

		class UnboundPropertyType
		{
			public string A { get; set; }
			public string B { get; set; }
			public string C { get; set; }
		}

		[Fact]
		public void UnboundPropertyFailsDefault()
		{
			var csvData = "A,B\n1,2\n3,4";
			var tr = new StringReader(csvData);
			var data = CsvDataReader.Create(tr);
			var ex = Assert.Throws<UnboundMemberException>(() => DataBinder.Create<UnboundPropertyType>(data));
			Assert.Contains("C", ex.UnboundProperties);
		}

		[Fact]
		public void UnboundColumnFailsConfigured()
		{
			var csvData = "A,B,C,D\n1,2\n3,4";
			var tr = new StringReader(csvData);
			var data = CsvDataReader.Create(tr);
			var opts = new DataBinderOptions { BindingMode = DataBindingMode.All };
			var ex = Assert.Throws<UnboundMemberException>(() => DataBinder.Create<UnboundPropertyType>(data, opts));
			Assert.Contains("D", ex.UnboundColumns);
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
			Assert.Equal(new[] { 7, 8, 9 }, item.Values.Values);
		}

		sealed class ManualBinder : IDataBinder<SeriesDateRecord>
		{
			readonly DataSeriesAccessor<DateType, int> series0;
			readonly int idIdx;
			readonly int nameIdx;

			public ManualBinder(ReadOnlyCollection<DbColumn> schema)
			{
				idIdx = schema.Single(c => c.ColumnName == "Id").ColumnOrdinal.Value;
				nameIdx = schema.Single(c => c.ColumnName == "Name").ColumnOrdinal.Value;
				var seriesCols =
					schema
					.Where(c => DateTime.TryParse(c.ColumnName, out _))
					.Select(c => new DataSeriesColumn<DateType>(c.ColumnName, DateType.Parse(c.ColumnName), c.ColumnOrdinal.Value));
				this.series0 = new DataSeriesAccessor<DateType, int>(seriesCols);
			}

			public void Bind(DbDataReader record, SeriesDateRecord item)
			{
				item.Id = record.GetInt32(idIdx);
				item.Name = record.GetString(nameIdx);
				item.Values = new Series<DateType, int>(this.series0, record);
			}

			public void Bind(DbDataReader record, object item)
			{
				Bind(record, (SeriesDateRecord)item);
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

		[Fact]
		public void BindContructorTests()
		{
			var dataStr = "Id,Name,Data,Version\n1,a,0x1234,1.2.3.4";
			var schema =
				new Schema.Builder()
				.Add<int>()
				.Add<string>()
				.Add<byte[]>()
				.Add<string>()
				.Build();

			var data = CsvDataReader.Create(new StringReader(dataStr), new CsvDataReaderOptions { Schema = new CsvSchema(schema), BinaryEncoding = BinaryEncoding.Hexadecimal });
			var binder = DataBinder.Create<Simple>(data, new DataBinderOptions { BindingMode = DataBindingMode.Any });
			Assert.True(data.Read());
			var r = binder.GetRecord(data);
			Assert.Equal(1, r.Id);
			Assert.Equal("a", r.Name);
			Assert.Equal(2, r.Data.data.Length);
			Assert.Equal(0x12, r.Data.data[0]);
			Assert.Equal(0x34, r.Data.data[1]);
			Assert.Equal(new Version(1, 2, 3, 4), r.Version);
		}

		[Fact]
		public void BindErrorTest()
		{
			var dataStr = "Id,Name,Date\n1,a,broken";
			var schema =
				new Schema.Builder()
				.Add<int>()
				.Add<string>()
				.Add<DateTime>()
				.Build();

			var data = CsvDataReader.Create(new StringReader(dataStr), new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			var binder = DataBinder.Create<MyDataRecord>(data, new DataBinderOptions { BindingMode = DataBindingMode.All });
			data.Read();
			var record = new MyDataRecord();
			var ex = Assert.Throws<DataBinderException>(() => binder.Bind(data, record));
			Assert.Equal(2, ex.Ordinal);
		}

		[Fact]
		public void DupeHeaderSuccess()
		{
			// the duplicate columns won't cause a problem, because it isn't part of the binding.
			var dataStr = "Id,Blorp,Name,Date,Blorp\n1,X,a,2000-11-12,Y";
			var schema =
				new Schema.Builder()
				.Add<int>("Id")
				.Add<string>("Blorp")
				.Add<string>("Name")
				.Add<DateTime>("Date")
				.Add<string>("Blorp")
				.Build();

			var data = CsvDataReader.Create(new StringReader(dataStr), new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			var binder = DataBinder.Create<MyDataRecord>(data, new DataBinderOptions { BindingMode = DataBindingMode.AllProperties });
			data.Read();
			var record = new MyDataRecord();
			binder.Bind(data, record);
			Assert.Equal(1, record.Id);
			Assert.Equal("a", record.Name);
			Assert.Equal(2000, record.Date.Value.Year);
		}

		[Fact]
		public void DupeHeaderFailure()
		{
			// the duplicate column causes a failure because we're trying to bind the name column.
			var dataStr = "Id,Name,Date,Name\n1,a,2000-11-12,Y";
			var schema =
				new Schema.Builder()
				.Add<int>("Id")
				.Add<string>("Name")
				.Add<DateTime>("Date")
				.Add<string>("Name")
				.Build();

			var data = CsvDataReader.Create(new StringReader(dataStr), new CsvDataReaderOptions { Schema = new CsvSchema(schema) });
			// cannot create a binder when the duplicate column is being mapped
			Assert.Throws<UnboundMemberException>(() => DataBinder.Create<MyDataRecord>(data, new DataBinderOptions { BindingMode = DataBindingMode.AllProperties }));
		}

#if NET6_0_OR_GREATER

		[Fact]
		public void BindDateOnlyTimeOnly()
		{
			var s = "Date,Time\n2020-01-01,13:14:15\n";
			var r = new StringReader(s);
			var data = CsvDataReader.Create(r);
			foreach (var d in data.GetRecords<DateTimeRecord>())
			{
				Assert.Equal(new DateOnly(2020, 1, 1), d.Date);
				Assert.Equal(new TimeOnly(13, 14, 15), d.Time);
			}
		}
#endif
	}

	class Simple
	{
		public int Id { get; private set; }
		public string Name { get; private set; }
		public SomeData Data { get; private set; }
		public Version Version { get; private set; }
	}

	class SomeData
	{
		public byte[] data;
		public SomeData(byte[] data)
		{
			this.data = data;
		}
	}

	class MyDataRecord
	{
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
		//[ColumnSeries("{Integer}")]
		public Series<int, int> Values { get; private set; }
	}

	class SeriesStringRecord
	{
		public int Id { get; private set; }
		public string Name { get; private set; }
		//[ColumnSeries("{Integer}")]
		public Series<string, int> Values { get; private set; }
	}

	class SeriesDateRecord
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public Series<DateType, int> Values { get; set; }
	}

	class Record
	{
		public int AccountNumber { get; set; }
		public int DayOfWeek { get; set; }
	}

	class NotMappedRecord
	{
		public int AccountNumber { get; set; }
		public int DayOfWeek { get; set; }
		[IgnoreDataMember]
		public string Name { get; set; }
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

	record class MyRecordClass
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public DateTime Date { get; set; }
	}

#if NET6_0_OR_GREATER

	class DateTimeRecord
	{
		public DateOnly Date { get; set; }

		public TimeOnly Time { get; set; }
	}

#endif
}
