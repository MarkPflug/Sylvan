using Sylvan.IO;
using System;
using System.Buffers;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
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
			var schema = new TypedCsvSchema();
			schema.Add(0, typeof(int));
			schema.Add(1, typeof(string));
			schema.Add(2, typeof(double?));
			schema.Add(3, typeof(DateTime));
			schema.Add(4, typeof(DateTime?));

			var options = new CsvDataReaderOptions
			{
				Schema = schema
			};

			using var tr = GetData();
			using var dr = CsvDataReader.Create(tr, options);
			var dt = new DataTable();
			dt.Load(dr);
		}

		static SqlConnection GetConnection()
		{
			var csb =
				new SqlConnectionStringBuilder
				{
					DataSource = @"(LocalDb)\MSSqlLocalDb",
					InitialCatalog = "Test",
					IntegratedSecurity = true
				};
			return new SqlConnection(csb.ConnectionString);
		}

		class SchemaProvider : ICsvSchemaProvider
		{
			readonly ReadOnlyCollection<DbColumn> schema;
			public SchemaProvider(ReadOnlyCollection<DbColumn> schema)
			{
				this.schema = schema;
			}

			public DbColumn GetColumn(string name, int ordinal)
			{
				return schema[ordinal];
			}
		}

		[Fact(Skip = "Usage example.")]
		public void SylvanRead()
		{
			using var tr = GetData();
			var options = new CsvDataReaderOptions { Delimiter = '|' };
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
		public void SqlTVPRawSample()
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

			cmd.CommandText = "InsertFeatures";
			cmd.CommandType = CommandType.StoredProcedure;
			var param = new SqlParameter()
			{
				ParameterName = "data",
				SqlDbType = SqlDbType.Structured
			};
			dataReader = new BoundedDataReader(dataReader, 100);
			param.Value = dataReader;
			cmd.Parameters.Add(param);
			cmd.ExecuteNonQuery();
		}


		[Fact(Skip = "Usage example.")]
		public void SqlTVPSample()
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
			cmd.CommandType = CommandType.StoredProcedure;
			var param = new SqlParameter()
			{
				ParameterName = "data",
				SqlDbType = SqlDbType.Structured
			};
			dataReader = new BoundedDataReader(dataReader, 100);
			var paramData = dataReader;
			param.Value = paramData;
			cmd.Parameters.Add(param);
			cmd.ExecuteNonQuery();
			cmd.CommandText = "commit tran";
			cmd.CommandType = CommandType.Text;
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

		static Schema GetSchema(SqlConnection conn, string table)
		{
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "select top 0 * from " + table;
			using var reader = cmd.ExecuteReader();
			var tableSchema = reader.GetColumnSchema();
			return new Schema(tableSchema);
		}

		[Fact(Skip = "Usage example.")]
		public void SqlTVPSimple2()
		{
			using var csvText = GetData(); // Gets a TextReader over a large-ish CSV dataset

			var conn = GetConnection();
			conn.Open();

			var data =
				Enumerable
				.Range(0, 10)
				.Select(i => new { Id = i, Name = "name " + i, Code = (i % 2 == 1) ? "" : "OR" });

			var dataReader = ObjectDataReader.Create(data);
			dataReader.AddColumn("Id", r => r.Id);
			dataReader.AddColumn("Name", r => r.Name);
			dataReader.AddColumn("Code", r => r.Code);

			using var cmd = conn.CreateCommand();
			cmd.CommandText = "InsertSimple2";
			cmd.CommandType = CommandType.StoredProcedure;
			var param = new SqlParameter()
			{
				ParameterName = "data",
				SqlDbType = SqlDbType.Structured
			};

			var paramData = dataReader;
			param.Value = paramData;
			cmd.Parameters.Add(param);
			cmd.ExecuteNonQuery();
		}

		[Fact(Skip = "Usage example.")]
		public void SqlTVPSimple2Csv()
		{
			using var csvText = GetData(); // Gets a TextReader over a large-ish CSV dataset

			var conn = GetConnection();
			conn.Open();

			var schema = GetSchema(conn, "Simple2");
			var reader = File.OpenText("Data/Simple2Data.csv");
			var dataReader = CsvDataReader.Create(reader, new CsvDataReaderOptions { Schema = schema });

			using var cmd = conn.CreateCommand();
			cmd.CommandText = "InsertSimple2";
			cmd.CommandType = CommandType.StoredProcedure;
			var param = new SqlParameter()
			{
				ParameterName = "data",
				SqlDbType = SqlDbType.Structured
			};

			var paramData = dataReader;
			param.Value = paramData;
			cmd.Parameters.Add(param);
			cmd.ExecuteNonQuery();
		}

		[Fact(Skip = "Usage example.")]
		public void SqlTVPSimple1()
		{
			using var csvText = GetData(); // Gets a TextReader over a large-ish CSV dataset

			var conn = GetConnection();
			conn.Open();

			var data =
				Enumerable
				.Range(0, 10)
				.Select(i => new { Id = i, Name = "name " + i });

			var dataReader = ObjectDataReader.Create(data);
			dataReader.AddColumn("Id", r => r.Id);
			dataReader.AddColumn("Name", r => r.Name);

			using var cmd = conn.CreateCommand();
			cmd.CommandText = "InsertSimple1";
			cmd.CommandType = CommandType.StoredProcedure;
			var param = new SqlParameter()
			{
				ParameterName = "data",
				SqlDbType = SqlDbType.Structured
			};

			var paramData = dataReader;
			param.Value = paramData;
			cmd.Parameters.Add(param);
			cmd.ExecuteNonQuery();
		}
	}
}
