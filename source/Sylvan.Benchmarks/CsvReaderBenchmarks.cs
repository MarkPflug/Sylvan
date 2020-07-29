using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using FlatFiles.TypeMapping;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	[SimpleJob(RuntimeMoniker.NetCoreApp31)]
	[MemoryDiagnoser]
	public class CsvReaderBenchmarks
	{
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
		public void NLightCsv()
		{
			var tr = TestData.GetTextReader();
			var dr = (IDataReader)new NLight.IO.Text.DelimitedRecordReader(tr, 0x10000);
			while (dr.Read())
			{
				for (int i = 0; i < dr.FieldCount; i++)
				{
					var s = dr.GetString(i);
				}
			}
		}

		[Benchmark]
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

		[Benchmark]
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

		[Benchmark]
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

		[Benchmark]
		public void NReco()
		{
			var tr = TestData.GetTextReader();
			var dr = new NReco.Csv.CsvReader(tr);
			dr.BufferSize = 0x10000;
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
			using var dr = await CsvDataReader.CreateAsync(tr);
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
			var types = new TypeCode[dr.FieldCount];
			
			for(int i = 0; i < types.Length; i++)
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
		public void NRecoSelect()
		{
			var tr = TestData.GetTextReader();
			var dr = new NReco.Csv.CsvReader(tr);
			dr.BufferSize = 0x10000;
			dr.Read(); // read the headers
			while (dr.Read())
			{
				var id = int.Parse(dr[0]);
				var name = dr[10];
				var val = int.Parse(dr[20]);
			}
		}

		[Benchmark]
		public async Task SylvanSelect()
		{
			var tr = TestData.GetTextReader();
			var dr = await CsvDataReader.CreateAsync(tr);
			while (await dr.ReadAsync())
			{
				var id = dr.GetInt32(0);
				var name = dr.GetString(10);
				var val = dr.GetInt32(20);
			}
		}
	}
}