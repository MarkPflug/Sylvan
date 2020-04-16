using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
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
		public void SylvanDataTable()
		{
			using var tr = GetData();
			using var dr = CsvDataReader.Create(tr);
			var dt = new DataTable();
			dt.Load(dr);
		}

		[Fact(Skip = "Usage example.")]
		public void SylvanRead()
		{
			using var tr = GetData();
			using var dr = CsvDataReader.Create(tr);
			while (dr.Read())
			{
				var id = dr.GetString(0);
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

			//dataReader = new EmptyAsNullDataReader(dataReader);

			var bcp = new SqlBulkCopy(conn);
			bcp.BulkCopyTimeout = 0;
			bcp.DestinationTableName = "Feature";
			bcp.BatchSize = 50000;
			bcp.WriteToServer(dataReader);
		}


		[Fact(Skip = "Usage example.")]
		public void SqlTableValueParameterSample()
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

			cmd.CommandText = "begin tran";
			cmd.ExecuteNonQuery();

			cmd.CommandText = "InsertFeatures";
			cmd.CommandType = System.Data.CommandType.StoredProcedure;
			var param = new SqlParameter()
			{
				ParameterName = "data",
				SqlDbType = System.Data.SqlDbType.Structured
			};
			//var paramData = GetDataRecords(new BoundedDataReader(dataReader, 100));
			var paramData = GetDataRecords(dataReader);
			param.Value = paramData;
			cmd.Parameters.Add(param);
			cmd.ExecuteNonQuery();
			cmd.CommandText = "commit tran";
			cmd.CommandType = System.Data.CommandType.Text;
			cmd.Parameters.Clear();
			cmd.ExecuteNonQuery();
		}

		static SqlDbType GetType(Type type)
		{
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return SqlDbType.Bit;
				case TypeCode.Int32:
					return SqlDbType.Int;
				case TypeCode.Double:
					return SqlDbType.Float;
				case TypeCode.DateTime:
					return SqlDbType.DateTime2;
				case TypeCode.String:
					return SqlDbType.NVarChar;
				default:
					throw new NotSupportedException();
			}
		}

		static SqlMetaData GetMetaData(DbColumn c)
		{
			if (c.DataType == typeof(string))
			{
				return new SqlMetaData(c.ColumnName, GetType(c.DataType), c.ColumnSize ?? 255);
			} else
			{
				return new SqlMetaData(c.ColumnName, GetType(c.DataType));
			}
		}

		static IEnumerable<SqlDataRecord> GetDataRecords(DbDataReader dr)
		{
			var schema = dr.GetColumnSchema();


			var cols =
			schema
				.Select(c => GetMetaData(c))
				.ToArray();
			var record = new SqlDataRecord(cols);

			while(dr.Read())
			{
				for(int i = 0; i < dr.FieldCount; i++)
				{
					var val = dr.GetValue(i);
					record.SetValue(i, val);
				}
				yield return record;
			}
		}
	}
}
