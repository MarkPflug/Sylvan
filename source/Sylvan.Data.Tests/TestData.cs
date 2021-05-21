using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

	public class ShippingRecord
	{
		public Guid RecordUID { get; set; }
		public string ProductName { get; set; }
		public int Quantity { get; set; }
		public DateTime ShipDate { get; set; }
		public double ShippedWeight { get; set; }
		public bool DeliveryConfirmed { get; set; }
	}

	public class TestData
	{
		const string DataSetSchema = @"RecordUID:Guid,ProductName,Quantity:int,ShipDate:DateTime,ShippedWeight:double,DeliveryConfirmed:bool";

		static ICsvSchemaProvider Schema;
		static CsvDataReaderOptions Options;

		static string CachedData;
		static byte[] CachedUtfData;

		static string Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ,;\"\'";

		static string GetRandomName(Random rand)
		{
			var len = rand.Next(8, 20);
			char[] buffer = new char[len];
			for(int i = 0; i < len; i++)
			{
				buffer[i] = Alphabet[rand.Next(0, Alphabet.Length)];
			}
			return new string(buffer);
			
		}

		static ShippingRecord CreateShippingRecord(Random rand)
		{
			var quantity = rand.Next(1, 12);
			return new ShippingRecord
			{
				RecordUID = Guid.NewGuid(),
				Quantity = quantity,
				DeliveryConfirmed = rand.Next(1, 5) % 4 < 2,
				ProductName = GetRandomName(rand),
				ShipDate = new DateTime(2015, 1, 1).AddDays(rand.NextDouble() * 365d * 7d),
				ShippedWeight = quantity * (rand.NextDouble() + 1d) * 4,
			};
		}

		static void CacheData()
		{
			var rand = new Random(1);

			var data =
				Enumerable.Range(0, 10000)
				.Select(i => CreateShippingRecord(rand))
				.ToArray();

			var reader = data.AsDataReader();
			var csvTW = new StringWriter();
			var csvWriter = CsvDataWriter.Create(csvTW);
			csvWriter.Write(reader);
			CachedData = csvTW.ToString();
			CachedUtfData = Encoding.UTF8.GetBytes(CachedData);
		}

		static TestData()
		{
			// is it a bad idea to do this in a static constructor?
			// probably, but this is only used in test/benchmarks.
			CacheData();
			Schema = new CsvSchema(Data.Schema.Parse(DataSetSchema).GetColumnSchema());
			Options = new CsvDataReaderOptions { Schema = Schema };
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
			var reader = File.OpenText("Data/Schema.csv");
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

	static class Extensions
	{
		public static T Repeat<T>(this T obj, Func<T, int, T> a, int count)
		{
			var item = obj;

			for (int i = 0; i < count; i++)
			{
				item = a(item, i);
			}

			return item;
		}
	}

	public static class DataReaderProcessor
	{
		public static void Process(this IDataReader reader)
		{
			while (reader.Read())
			{
				reader.ProcessRecord();
			}
		}

		public static async Task ProcessAsync(this DbDataReader reader)
		{
			while (await reader.ReadAsync())
			{
				reader.ProcessRecord();
			}
		}

		public static void ProcessRecord(this IDataRecord record)
		{
			for (int i = 0; i < record.FieldCount; i++)
			{
				if (record.IsDBNull(i))
					continue;

				switch (Type.GetTypeCode(record.GetFieldType(i)))
				{
					case TypeCode.Boolean:
						record.GetBoolean(i);
						break;
					case TypeCode.Int32:
						record.GetInt32(i);
						break;
					case TypeCode.DateTime:
						record.GetDateTime(i);
						break;
					case TypeCode.Double:
						record.GetDouble(i);
						break;
					case TypeCode.Decimal:
						record.GetDecimal(i);
						break;
					case TypeCode.String:
						record.GetString(i);
						break;
					default:
						continue;
				}
			}
		}
	}
}
