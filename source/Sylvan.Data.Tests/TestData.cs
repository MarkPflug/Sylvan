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
		public Guid RecordId { get; set; }
		public string ProductName { get; set; }
		public int Quantity { get; set; }
		public DateTime ShipDate { get; set; }
		public double ShippedWeight { get; set; }
		public bool DeliveryConfirmed { get; set; }
	}

	public class TestData
	{
		const string DataSetSchema = @"RecordId:Guid,ProductName,Quantity:int,ShipDate:DateTime,ShippedWeight:double,DeliveryConfirmed:bool";

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

		static ShippingRecord CreateShippingRecord(Random rand, int i)
		{
			var quantity = rand.Next(1, 12);
			return new ShippingRecord
			{
				RecordId = Guid.NewGuid(),
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
				Enumerable.Range(0, 500000)
				.Select(i => CreateShippingRecord(rand, i))
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
			var schema = Data.Schema.Parse(DataSetSchema);
			Schema = new CsvSchema(schema.GetColumnSchema());
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
			return CsvDataReader.Create(reader, new CsvDataReaderOptions() { Schema = TestDataSchema });
		}

		public static ICsvSchemaProvider TestDataSchema => new CsvSchema(Sylvan.Data.Schema.Parse(DataSetSchema));

		class TypedCsvColumn : DbColumn
		{
			public TypedCsvColumn(Type type, bool allowNull)
			{
				this.DataType = type;
				this.AllowDBNull = allowNull;
			}
		}

		static ObjectDataReader.Builder<TestRecord> Builder =
			ObjectDataReader
				.CreateBuilder<TestRecord>()
				.AddColumn("Id", i => i.Id)
				.AddColumn("Name", i => i.Name)
				.AddColumn("Date", i => i.Date)
				.AddColumn("IsActive", i => i.IsActive)
				.Repeat((b, i) => b.AddColumn("Data" + i, r => r.DataSet[i]), 10);


		public static DbDataReader GetTestData(int count = 10)
		{
			return Builder.Build(GetTestObjects(count, 10));
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

		static ObjectDataReader.Builder<BinaryData> BinaryBuilder =
			ObjectDataReader
				.CreateBuilder<BinaryData>()
				.AddColumn("Id", d => d.Id)
				.AddColumn("Data", d => d.Data);

		public static DbDataReader GetBinaryData()
		{
			return BinaryBuilder.Build(GetTestBinary());
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
				.CreateBuilder<TestRecord>()
				.AddColumn("Id", i => i.Id)
				.AddColumn("Name", i => i.Name)
				.AddColumn("Date", i => i.Date)
				.AddColumn("IsActive", i => i.IsActive)
				.Repeat((b, i) => b.AddColumn("Data" + i, r => r.DataSet[i]), valueCount)
				.Build(items);
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
			while (await reader.ReadAsync().ConfigureAwait(false))
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
