using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;

namespace Sylvan.Data.Csv
{
	static class TestData
	{
		const string DataSetUrl = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/f7c2384622806d5297d16c314a7bc0b9cde24937/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv";
		const string DataFileName = "Data.csv";

		static string CachedData;

		static void CacheData()
		{
			if (!File.Exists(DataFileName))
			{
				using var oStream = File.OpenWrite(DataFileName);
				using var iStream = new HttpClient().GetStreamAsync(DataSetUrl).Result;
				iStream.CopyTo(oStream);
			}
			CachedData = File.ReadAllText(DataFileName);
		}

		static TestData()
		{
			// is it a bad idea to do this in a static constructor?
			// probably, but this is only used in test/benchmarks.
			CacheData();
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

		public static DbDataReader GetData()
		{
			return CsvDataReader.Create(GetTextReader());
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
				Type i = typeof(int), s = typeof(string), f = typeof(double);
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

		public sealed class TestClass
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public DateTime Date { get; set; }
			public bool IsActive { get; set; }
			public double[] DataSet { get; set; }
		}

		public const int DefaultRecordCount = 100000;
		public const int DefaultDataValueCount = 20;

		public static IEnumerable<TestClass> GetTestObjects(int recordCount = DefaultRecordCount, int valueCount = DefaultDataValueCount)
		{
			// We'll reuse the single instance of TestClass. 
			// We do this so memory usage in benchmarks is a better indicator
			// of the library, and not just overwhelmed by TestClass allocations.
			var row = new TestClass();
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

		public static DbDataReader GetBinaryData()
		{
			var items = GetTestBinary();
			var reader = ObjectDataReader.Create(items);
			reader.AddColumn("Id", d => d.Id);
			reader.AddColumn("Data", d => d.Data);
			return reader;
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
			var reader = ObjectDataReader.Create(items);
			reader.AddColumn("Id", d => d.Id);
			reader.AddColumn("Name", d => d.Name);
			reader.AddColumn("Date", d => d.Date);
			reader.AddColumn("IsActive", d => d.IsActive);

			for (int i = 0; i < valueCount; i++)
			{
				var idx = 0;
				reader.AddColumn("Value" + i, d => d.DataSet[idx]);
			}

			return reader;
		}
	}
}
