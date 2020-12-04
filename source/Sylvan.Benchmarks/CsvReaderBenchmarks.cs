using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	[MemoryDiagnoser]
	public class CsvReaderBenchmarks
	{
		const int BufferSize = 0x4000;
		readonly StringFactory pool;
		char[] buffer = new char[BufferSize];

		public CsvReaderBenchmarks()
		{
			this.pool = new StringPool().GetString;
		}
	
		[Benchmark]
		public void NaiveBroken()
		{
			var tr = TestData.GetTextReader();
			string line;
			while ((line = tr.ReadLine()) != null)
			{
				var cols = line.Split(',');
				for (int i = 0; i < cols.Length; i++)
				{
					var s = cols[i];
				}
			}
		}

		[Benchmark]
		public async Task Sylvan()
		{
			using var tr = TestData.GetTextReader();
			using var dr = await CsvDataReader.CreateAsync(tr, new CsvDataReaderOptions() { Buffer = buffer });
			while (await dr.ReadAsync())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetString(i);
				}
			}
		}

		[Benchmark]
		public async Task SylvanShare()
		{
			using var tr = TestData.GetTextReader();
			using var dr = await CsvDataReader.CreateAsync(tr);
			var hs = new HashSet<string>();
			while (await dr.ReadAsync())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetString(i);
					hs.Add(s);

				}
			}
		}

		[Benchmark]
		public async Task SylvanDeDupe()
		{
			using var tr = TestData.GetTextReader();
			var pool = new StringPool();
			var opts = new CsvDataReaderOptions { StringFactory = pool.GetString };
			using var dr = await CsvDataReader.CreateAsync(tr, opts);
			while (await dr.ReadAsync())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetString(i);
				}
			}
		}

		[Benchmark]
		public async Task SylvanDeDupeReuse()
		{
			using var tr = TestData.GetTextReader();
			var opts = new CsvDataReaderOptions { StringFactory = pool };
			using var dr = await CsvDataReader.CreateAsync(tr, opts);
			while (await dr.ReadAsync())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetString(i);
				}
			}
		}

		[Benchmark]
		public async Task SylvanSchema()
		{
			using var tr = TestData.GetTextReader();
			using var dr = await CsvDataReader.CreateAsync(tr, new CsvDataReaderOptions { Schema = TestData.TestDataSchema });
			await ProcessDataAsync(dr);
		}

		[Benchmark]
		public void SylvanSchemaDeDupe()
		{
			var pool = new StringPool();
			using var tr = TestData.GetTextReader();
			using var dr = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = TestData.TestDataSchema, StringFactory = pool.GetString });
			ProcessData(dr);
		}

		static void ProcessData(CsvDataReader dr)
		{
			var types = new TypeCode[dr.FieldCount];

			for (int i = 0; i < types.Length; i++)
			{
				types[i] = Type.GetTypeCode(dr.GetFieldType(i));
			}
			while (dr.Read())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					switch (types[i])
					{
						case TypeCode.Int32:
							var v = dr.GetInt32(i);
							break;
						case TypeCode.Double:
							if (i == 4 && dr.IsDBNull(i))
								break;
							var d = dr.GetDouble(i);
							break;
						case TypeCode.String:
							var s = dr.GetString(i);
							break;
						default:
							break;
					}
				}
			}
		}

		static async Task ProcessDataAsync(CsvDataReader dr)
		{
			var types = new TypeCode[dr.FieldCount];

			for (int i = 0; i < types.Length; i++)
			{
				types[i] = Type.GetTypeCode(dr.GetFieldType(i));
			}
			while (await dr.ReadAsync())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					switch (types[i])
					{
						case TypeCode.Int32:
							var v = dr.GetInt32(i);
							break;
						case TypeCode.Double:
							if (i == 4 && dr.IsDBNull(i))
								break;
							var d = dr.GetDouble(i);
							break;
						case TypeCode.String:
							var s = dr.GetString(i);
							break;
						default:
							break;
					}
				}
			}
		}

		[Benchmark]
		public void SylvanSelect()
		{
			using var tr = TestData.GetTextReader();
			using var dr = CsvDataReader.Create(tr);
			while (dr.Read())
			{
				var id = dr.GetInt32(0);
				var name = dr.GetString(10);
				var val = dr.GetInt32(20);
			}
		}
	}

}
