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

		//[Fact(Skip = "Usage example.")]
		[Fact]
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
			csvSchema = null;
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

	class EmptyAsNullDataReader : DbDataReader
	{
		DbDataReader dr;

		public EmptyAsNullDataReader(DbDataReader dr)
		{
			this.dr = dr;
		}

		public override object this[int ordinal] => dr[ordinal];

		public override object this[string name] => dr[name];

		public override int Depth => dr.Depth;

		public override int FieldCount => dr.FieldCount;

		public override bool HasRows => dr.HasRows;

		public override bool IsClosed => dr.IsClosed;

		public override int RecordsAffected => dr.RecordsAffected;

		public override bool GetBoolean(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override byte GetByte(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override char GetChar(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override string GetDataTypeName(int ordinal)
		{
			return dr.GetDataTypeName(ordinal);
		}

		public override DateTime GetDateTime(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override decimal GetDecimal(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override double GetDouble(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}

		public override Type GetFieldType(int ordinal)
		{
			return dr.GetFieldType(ordinal);
		}

		public override float GetFloat(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override Guid GetGuid(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override short GetInt16(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override int GetInt32(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetInt64(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override string GetName(int ordinal)
		{
			return dr.GetName(ordinal);
		}

		public override int GetOrdinal(string name)
		{
			return dr.GetOrdinal(name);
		}

		public override string GetString(int ordinal)
		{
			var str = dr.GetString(ordinal);
			return str?.Length == 0 ? null : str;
		}

		public override object GetValue(int ordinal)
		{
			return this.GetString(ordinal);
		}

		public override int GetValues(object[] values)
		{
			return dr.GetValues(values);
		}

		public override bool IsDBNull(int ordinal)
		{
			return dr.IsDBNull(ordinal);
		}

		public override bool NextResult()
		{
			return dr.NextResult();
		}

		public override bool Read()
		{
			return dr.Read();
		}
	}
}
