using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class SampleTests
	{
		const string DataSetUrl = "https://geonames.usgs.gov/docs/stategaz/NationalFile.zip";
		const string ZipFileName = "GnisData.zip";
		const string DataFileName = "GnisData.csv";

		public TextReader GetData()
		{
			// download and unzip the dataset if it doesn't already exist.
			if (!File.Exists(DataFileName))
			{
				if (!File.Exists(ZipFileName))
				{
					using var oStream = File.OpenWrite(ZipFileName);
					using var iStream = new HttpClient().GetStreamAsync(DataSetUrl).Result;
					iStream.CopyTo(oStream);
				}

				{
					using var zipStream = File.OpenRead(ZipFileName);
					using var oStream = File.Create(DataFileName);
					var zip = new ZipArchive(zipStream, ZipArchiveMode.Read);
					var entry = zip.Entries.Single();
					using var entryStream = entry.Open();
					entryStream.CopyTo(oStream);
				}
			}
			return File.OpenText(DataFileName);
		}

		sealed class Schema : ICsvSchemaProvider
		{
			readonly ReadOnlyCollection<DbColumn> schema;
			public Schema(ReadOnlyCollection<DbColumn> schema)
			{
				this.schema = schema;
			}

			public DbColumn GetColumn(string name, int ordinal)
			{
				return schema[ordinal];
			}
		}

		[Fact(Skip = "Usage example.")]
		public void SqlBulkLoadSample()
		{
			using var csvText = GetData(); // Gets a TextReader over a large-ish CSV dataset

			var csb =
				new SqlConnectionStringBuilder
				{
					DataSource = @"(LocalDb)\MSSqlLocalDb",
					InitialCatalog = "Test",
					IntegratedSecurity = true
				};
			using var conn = new SqlConnection(csb.ConnectionString);
			conn.Open();

			var cmd = conn.CreateCommand();
			cmd.CommandText = "select top 0 * from Feature";
			var reader = cmd.ExecuteReader();
			var tableSchema = reader.GetColumnSchema();
			reader.Close();
			var csvSchema = new Schema(tableSchema);
			
			var options = new CsvDataReaderOptions
			{
				Delimiter = '|',
				Schema = csvSchema,
			};

			DbDataReader dataReader = CsvDataReader.Create(csvText, options);

			dataReader = new EmptyAsNullDataReader(dataReader);

			var bcp = new SqlBulkCopy(conn);
			bcp.BulkCopyTimeout = 0;
			bcp.DestinationTableName = "Feature";
			bcp.BatchSize = 50000;
			bcp.WriteToServer(dataReader);
		}
	}
}
