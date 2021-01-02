﻿using BenchmarkDotNet.Attributes;
using Sylvan.Data;
using Sylvan.Data.Csv;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sylvan.Benchmarks
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
			await dr.ProcessDataAsync();
		}

		[Benchmark]
		public void SylvanSchemaDeDupe()
		{
			var pool = new StringPool();
			using var tr = TestData.GetTextReader();
			using var dr = CsvDataReader.Create(tr, new CsvDataReaderOptions { Schema = TestData.TestDataSchema, StringFactory = pool.GetString });
			dr.ProcessData();
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
