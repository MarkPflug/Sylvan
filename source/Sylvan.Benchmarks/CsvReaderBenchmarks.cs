using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using FlatFiles;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

using Cesil;

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

		[Benchmark(Baseline = true)]
		public void CsvHelper()
		{
			var tr = TestData.GetTextReader();
			var csv = new CsvHelper.CsvDataReader(new CsvHelper.CsvReader(tr, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.CurrentCulture)));
			var dr = (IDataReader)csv;
			while (dr.Read())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetString(i);
				}
			}
		}

		[Benchmark]
		public void CesilCsv()
		{
			var tr = TestData.GetTextReader();

			var csv = new CsvHelper.CsvDataReader(new CsvHelper.CsvReader(tr, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.CurrentCulture)));
			var dr = (IDataReader)csv;
			while (dr.Read())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetString(i);
				}
			}
		}


		[Benchmark]
		public void CsvTextFieldParser()
		{
			var tr = TestData.GetTextReader();
			var csv = new NotVisualBasic.FileIO.CsvTextFieldParser(tr);			
			while(!csv.EndOfData)
			{
				var fields = csv.ReadFields();
			}
		}

		//[Benchmark]
		public void FastCsvParser()
		{
			var s = TestData.GetUtf8Stream();
			var csv = new CsvParser.CsvReader(s, System.Text.Encoding.UTF8);
			while (csv.MoveNext())
			{
				var row = csv.Current;
				for (int i = 0; i < row.Count; i++)
				{
					var str = row[i];
				}
			}
		}

		//[Benchmark]
		public void CsvBySteve()
		{
			var s = TestData.GetUtf8Stream();
			var rows = global::Csv.CsvReader.ReadFromStream(s);

			foreach (var row in rows)
			{
				for (int i = 0; i < row.ColumnCount; i++)
				{
					var str = row[i];
				}
			}
		}

		// BenchmarkDotnet refuses to load this assembly, not sure why
		//[Benchmark]
		//public void Lumenworks()
		//{
		//	var tr = new StringReader(data);
		//	var csv = new CsvReader(tr, true);
		//	var dr = (IDataReader)csv;
		//	long l = 0;
		//	while (dr.Read())
		//	{
		//		for (int i = 0; i < dr.FieldCount; i++)
		//		{
		//			var s = dr.GetString(i);
		//			l += s.Length;
		//		}
		//	}
		//}

		//[Benchmark]
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

		//[Benchmark]
		//public void NLightCsv()
		//{
		//	var tr = TestData.GetTextReader();
		//	var dr = (IDataReader)new NLight.IO.Text.DelimitedRecordReader(tr, 0x10000);
		//	while (dr.Read())
		//	{
		//		for (int i = 0; i < dr.FieldCount; i++)
		//		{
		//			var s = dr.GetString(i);
		//		}
		//	}
		//}

		//[Benchmark]
		public void VisualBasic()
		{
			var tr = TestData.GetTextReader();
			var dr = new TextFieldParser(tr);
			dr.SetDelimiters(",");
			dr.HasFieldsEnclosedInQuotes = true;
			while (!dr.EndOfData)
			{
				var cols = dr.ReadFields();
				for (int i = 0; i < cols.Length; i++)
				{
					var s = cols[i];
				}
			}
		}

		//[Benchmark]
		public void OleDbCsv()
		{
			//Requires: https://www.microsoft.com/en-us/download/details.aspx?id=54920
			var connString = string.Format(
				@"Provider=Microsoft.ACE.OLEDB.12.0; Data Source={0};Extended Properties=""Text;HDR=YES;FMT=Delimited""",
				Path.GetDirectoryName(Path.GetFullPath(TestData.DataFile))
			);
			using (var conn = new OleDbConnection(connString))
			{
				conn.Open();
				var cmd = conn.CreateCommand();
				cmd.CommandText = "SELECT * FROM [" + Path.GetFileName(TestData.DataFile) + "]";
				var dr = cmd.ExecuteReader();
				while (dr.Read())
				{
					for (int i = 0; i < dr.FieldCount; i++)
					{
						var s = dr.GetValue(i).ToString();
					}
				}
			}
		}

		//[Benchmark]
		public void FlatFilesCsv()
		{
			var tr = TestData.GetTextReader();
			var opts = new FlatFiles.SeparatedValueOptions() { IsFirstRecordSchema = true };
			var dr = new FlatFiles.FlatFileDataReader(new FlatFiles.SeparatedValueReader(tr, opts));

			while (dr.Read())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetValue(i);
				}
			}
		}

		//[Benchmark]
		public void FSharpData()
		{
			var tr = TestData.GetTextReader();
			var csv = FSharp.Data.CsvFile.Load(tr);

			foreach (var row in csv.Rows)
			{
				for (int i = 0; i < row.Columns.Length; i++)
				{
					var s = row.Columns[i];
				}
			}
		}


		[Benchmark]
		public void NReco()
		{
			var tr = TestData.GetTextReader();
			var dr = new NReco.Csv.CsvReader(tr);
			dr.BufferSize = BufferSize;
			dr.Read(); // read the headers
			while (dr.Read())
			{
				for (int i = 0; i < dr.FieldsCount; i++)
				{
					var s = dr[i];
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

		//[Benchmark]
		public void NRecoSelect()
		{
			using var tr = TestData.GetTextReader();
			var dr = new NReco.Csv.CsvReader(tr);
			dr.BufferSize = BufferSize;
			dr.Read(); // read the headers
			while (dr.Read())
			{
				var id = int.Parse(dr[0]);
				var name = dr[10];
				var val = int.Parse(dr[20]);
			}
		}

		//[Benchmark]
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
