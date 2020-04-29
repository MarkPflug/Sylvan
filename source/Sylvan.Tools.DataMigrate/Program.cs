using Microsoft.Data.SqlClient;
using Sylvan.Data;
using Sylvan.Data.Csv;
using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Sylvan.Tools.DataMigrate
{
	class Program
    {
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
				new CsvWriterOptions {
					BufferSize = 10000000,
					TrueString = "1",
					FalseString = "0",
				};

			using var conn = new SqlConnection(csb.ConnectionString);
			conn.Open();
			using var f = File.Create("data.zip");
			using var za = new ZipArchive(f, ZipArchiveMode.Create);

			var cmd = conn.CreateCommand();
			cmd.CommandText = "select table_schema, table_name from information_schema.tables where table_type = 'BASE TABLE'";
			using var reader = cmd.ExecuteReader();

			var dc = conn.CreateCommand();
			while (reader.Read())
			{
				
				var schema = reader.GetString(0);
				var tableName = reader.GetString(1);
				Console.WriteLine($"Exporting {schema}.{tableName}");
				var e = za.CreateEntry(tableName + ".csv");
				using var os = e.Open();

				dc.CommandText = "select * from [" + schema + "].[" + tableName + "]";
				using var dr = dc.ExecuteReader();

				using var ts = new StreamWriter(os, Encoding.UTF8);
				var csvw = new CsvDataWriter(ts, opts);
				csvw.WriteAsync(dr).Wait();
			}
		}


    }
}
