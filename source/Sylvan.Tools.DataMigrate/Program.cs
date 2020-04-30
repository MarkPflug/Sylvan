using Microsoft.Data.SqlClient;
using Sylvan.Data;
using Sylvan.Data.Csv;
using Sylvan.Terminal;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace Sylvan.Tools.DataMigrate
{
	class Program
	{
		class SqlTableInfo
		{
			public SqlTableInfo(string schema, string name, long rowCount, long size)
			{
				this.Schema = schema;
				this.Name = name;
				this.RowCount = rowCount;
				this.Size = size;

			}
			public string Schema { get; }
			public string Name { get; }
			public long RowCount { get; }
			public long Size { get; }

			public override string ToString()
			{
				return $"{Schema}.{Name} RowCount: {RowCount} RowCount: {Size}";
			}
		}

		static void Main(string[] args)
		{
			var csb = new SqlConnectionStringBuilder
			{
				DataSource = ".",
				InitialCatalog = "ccexport",
				IntegratedSecurity = true,
				MultipleActiveResultSets = true,
			};

			var opts =
				new CsvWriterOptions
				{
					BufferSize = 10000000,
					TrueString = "1",
					FalseString = "0",
				};

			using var conn = new SqlConnection(csb.ConnectionString);
			conn.Open();
			using var f = File.Create("data.zip");
			using var za = new ZipArchive(f, ZipArchiveMode.Create);

			var cmd = conn.CreateCommand();

			cmd.CommandText = File.ReadAllText("Data\\SqlServerDataSizeQuery.sql");
			var tableInfo = new List<SqlTableInfo>();

			var cc = new VirtualTerminalWriter(Console.Out);

			using (var reader = cmd.ExecuteReader())
			{
				while (reader.Read())
				{
					string schema = reader.GetString(0);
					string table = reader.GetString(1);
					var rowCount = reader.GetInt64(2);
					var size = reader.GetInt64(3);
					tableInfo.Add(new SqlTableInfo(schema, table, rowCount, size));
				}
			}

			long totalSize = tableInfo.Sum(t => t.Size);

			long progress = 0;

			long updateT = totalSize / 1000;

			long pp = 0;
			var sw = Stopwatch.StartNew();
			{
				cmd.CommandText = "select table_schema, table_name from information_schema.tables where table_type = 'BASE TABLE'";
				using var reader = cmd.ExecuteReader();

				var dc = conn.CreateCommand();
				while (reader.Read())
				{
					var schema = reader.GetString(0);
					var tableName = reader.GetString(1);
					var c = StringComparer.OrdinalIgnoreCase;
					var info = tableInfo.FirstOrDefault(t => c.Equals(schema, t.Schema) && c.Equals(tableName, t.Name));
					long rowInc = info.RowCount == 0 ? 0 : info.Size / info.RowCount;
					cc.SetCursorPosition(0, 1);
					cc.WriteLine($"                                                                                                          ");
					cc.SetCursorPosition(0, 1);
					cc.WriteLine($"Exporting {schema}.{tableName}");
					var e = za.CreateEntry(tableName + ".csv");
					using var os = e.Open();

					dc.CommandText = "select * from [" + schema + "].[" + tableName + "]";
					using var dr = dc.ExecuteReader();

					using var ts = new StreamWriter(os, Encoding.UTF8);
					Action<long> cb = (r) => { 
						progress += rowInc; 
						pp += rowInc; 
						if (pp > updateT) { 
							pp -= updateT;
							
							var p = progress * 100.0 / totalSize;
							
							var e = sw.Elapsed;
							var total = e.Ticks * totalSize / progress;
							var time = TimeSpan.FromTicks(total);
					
							cc.SetCursorPosition(0, 2);
							cc.WriteLine($"                                                                                                          ");
							cc.SetCursorPosition(0, 2);
							cc.Write($"{p,5:0.0}% {(int)e.TotalSeconds}/{(int)time.TotalSeconds}");
						} 
					};
					var csvw = new CsvDataWriter(ts, opts, cb);
					csvw.Write(dr);
				}
			}
		}
	}
}
