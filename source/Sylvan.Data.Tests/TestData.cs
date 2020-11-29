using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace Sylvan.Data
{
	public sealed class TestRecord
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public DateTime Date { get; set; }
		public bool IsActive { get; set; }
		public double[] DataSet { get; set; }
	}

	public class Monster
	{
		public string Name { get; set; }
		public int Health { get; set; }
		public int Armor { get; set; }
		public int Strength { get; set; }
		public int Agility { get; set; }
		public int Intellect { get; set; }
	}

	public class CovidRecord
	{
		public int UID { get; set; }
		public string iso2 { get; set; }
		public string iso3 { get; set; }
		public int? code3 { get; set; }
		public float? FIPS { get; set; }
		public string Admin2 { get; set; }
		public string Province_State { get; set; }
		public string Country_Region { get; set; }
		public float? Lat { get; set; }
		public float? Long_ { get; set; }
		public string Combined_Key { get; set; }
	}

	public class TestData
	{
		const string DataSetUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/f7c2384622806d5297d16c314a7bc0b9cde24937/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv";
		const string DataFileName = "Data.csv";

		const string DataSetSchema = @"UID:int,
iso2,
iso3,
code3:int?,
FIPS:float?,
Admin2,
Province_State,
Country_Region,
Lat:float?,
Long_:float?,
Combined_Key,
{Date}>Values*:int";

		static ICsvSchemaProvider Schema;
		static CsvDataReaderOptions Options;

		static string CachedData;
		static byte[] CachedUtfData;

		static void CacheData()
		{
			if (!File.Exists(DataFileName))
			{
				using var oStream = File.OpenWrite(DataFileName);
				using var iStream = new HttpClient().GetStreamAsync(DataSetUrl).Result;
				iStream.CopyTo(oStream);
			}
			CachedData = File.ReadAllText(DataFileName);
			CachedUtfData = Encoding.UTF8.GetBytes(CachedData);
		}

		static TestData()
		{
			// is it a bad idea to do this in a static constructor?
			// probably, but this is only used in test/benchmarks.
			CacheData();
			Schema = new CsvSchema(Sylvan.Data.Schema.TryParse(DataSetSchema).GetColumnSchema());
			Options = new CsvDataReaderOptions { Schema = Schema };
		}

		public static string DataFile
		{
			get
			{
				return DataFileName;
			}
		}

		public static TextReader GetTextReader()
		{
			return new StringReader(CachedData);
		}

		public static Stream GetUtf8Stream()
		{
			return new MemoryStream(CachedUtfData);
		}

		public static DbDataReader GetData()
		{

			return CsvDataReader.Create(GetTextReader());
		}

		public static DbDataReader GetDataWithSchema(Action<CsvDataReaderOptions> opts = null)
		{
			opts?.Invoke(Options);
			return CsvDataReader.Create(GetTextReader(), Options);
		}

		public static DbDataReader GetTypedData()
		{ 
			var reader = File.OpenText("Data\\Schema.csv");
			return CsvDataReader.Create(reader, new CsvDataReaderOptions() { Schema = DataSchema.Instance });
		}

		public static ICsvSchemaProvider TestDataSchema => DataSchema.Instance;


		class DataSchema : ICsvSchemaProvider
		{
			public static DataSchema Instance = new DataSchema();
			Type[] types;
			bool[] nullable;

			private DataSchema()
			{
				Type i = typeof(int), s = typeof(string), f = typeof(float);
				types = new Type[] { i, s, s, i, f, s, s, s, f, f, s };
				nullable = new bool[] { false, false, false, false, true };
			}

			public DbColumn GetColumn(string name, int ordinal)
			{
				Type type =
					ordinal < types.Length
					? types[ordinal]
					: typeof(int);
				bool allowNull =
					ordinal < nullable.Length
					? nullable[ordinal]
					: false;
				return new TypedCsvColumn(type, allowNull);
			}
		}

		class TypedCsvColumn : DbColumn
		{
			public TypedCsvColumn(Type type, bool allowNull)
			{
				this.DataType = type;
				this.AllowDBNull = allowNull;
			}
		}

		static ObjectDataReader.Factory<TestRecord> Factory =
			ObjectDataReader
				.BuildFactory<TestRecord>()
				.AddColumn("Id", i => i.Id)
				.AddColumn("Name", i => i.Name)
				.AddColumn("Date", i => i.Date)
				.AddColumn("IsActive", i => i.IsActive)
				.Repeat((b, i) => b.AddColumn("Data" + i, r => r.DataSet[i]), 10)
				.Build();


		public static DbDataReader GetTestData(int count = 10)
		{
			return Factory.Create(GetTestObjects(count, 10));
		}

		public const int DefaultRecordCount = 100000;
		public const int DefaultDataValueCount = 20;

		public static IEnumerable<TestRecord> GetTestObjects(int recordCount = DefaultRecordCount, int valueCount = DefaultDataValueCount)
		{
			// We'll reuse the single instance of TestClass. 
			// We do this so memory usage in benchmarks is a better indicator
			// of the library, and not just overwhelmed by TestClass allocations.
			var row = new TestRecord();
			DateTime startDate = new DateTime(2020, 3, 23, 0, 0, 0, DateTimeKind.Utc);
			row.DataSet = new double[valueCount];
			var counter = 1;

			return
				Enumerable
				.Range(0, recordCount)
				.Select(
					i =>
					{
						row.Id = i;
						row.Name = "Model Number: 1337";
						row.Date = startDate.AddDays(i);
						row.IsActive = i % 2 == 1;
						for (int idx = 0; idx < row.DataSet.Length; idx++)
						{
							row.DataSet[idx] = .25 * counter++;
						}
						return row;
					}
				);
		}

		static ObjectDataReader.Factory<BinaryData> BinaryFactory =
			ObjectDataReader
				.BuildFactory<BinaryData>()
				.AddColumn("Id", d => d.Id)
				.AddColumn("Data", d => d.Data)
				.Build();

		public static DbDataReader GetBinaryData()
		{
			return BinaryFactory.Create(GetTestBinary());
		}

		public class BinaryData
		{
			public int Id { get; set; }
			public byte[] Data { get; set; }
		}

		public static IEnumerable<BinaryData> GetTestBinary()
		{
			yield return new BinaryData { Id = 1, Data = new byte[] { 1, 2, 3, 4, 5 } };
			yield return new BinaryData { Id = 2, Data = new byte[] { 5, 4, 3, 2, 1 } };

		}

		public static DbDataReader GetTestDataReader(int recordCount = DefaultRecordCount, int valueCount = DefaultDataValueCount)
		{
			var items = GetTestObjects(recordCount, valueCount);
			return
				ObjectDataReader
				.BuildFactory<TestRecord>()
				.AddColumn("Id", i => i.Id)
				.AddColumn("Name", i => i.Name)
				.AddColumn("Date", i => i.Date)
				.AddColumn("IsActive", i => i.IsActive)
				.Repeat((b, i) => b.AddColumn("Data" + i, r => r.DataSet[i]), valueCount)
				.Build()
				.Create(items);
		}
	}
}
