using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Sylvan.CommandLine;
using Sylvan.Data;
using Sylvan.Data.Csv;
using Sylvan.Terminal;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

		static void InitializeProviders()
		{
			DbProviderFactories.RegisterFactory("MySqlConnector", MySqlConnectorFactory.Instance);
			DbProviderFactories.RegisterFactory("Npgsql", NpgsqlFactory.Instance);
			DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", SqlClientFactory.Instance);
		}

		static Task<int> Main(string[] args)
		{

			var cmdline = Environment.CommandLine;

			var program = new Program();

			InitializeProviders();


			var cmd = new RootCommand("migrate")
			{
				new Option<string>("--source", description: "The source to copy data from."),
				new Option<string>("--sourceProvider", description: "The source data provider name."),
				new Option<string>("--destination", description: "The destination to copy data to."),
				new Option<string>("--destinationProvider", description: "The destination data provider name.")
			};

			var handler =
				CommandHandler.Create<string, string,string,string>(program.Migrate);
			
			cmd.Handler = handler;

			return cmd.InvokeAsync(args);
		}

		[Command]
		void Migrate(string sourceProvider, string srcConnectionString, string dstProviderName, string dstConnectionString)
		{
			var srcProvider = DbProviderFactories.GetFactory(sourceProvider);
			var dstProvider = DbProviderFactories.GetFactory(dstProviderName);

			var srcCSB = srcProvider.CreateConnectionStringBuilder();
			srcCSB.ConnectionString = srcConnectionString;

			var dstCSB = dstProvider.CreateConnectionStringBuilder();
			dstCSB.ConnectionString = dstConnectionString;
		}

		void WriteTo(DbConnection conn, DbDataReader data, string tableName)
		{
			
			switch (conn) {
				case SqlConnection msSql:
					WriteTo(msSql, data, tableName);
					break;
				case MySqlConnection mySql:
					WriteTo(mySql, data);
					break;
				case NpgsqlConnection npgsqlConn:
					WriteTo(npgsqlConn, data);
					break;
				default:
					throw new NotSupportedException();
			}
		}

		void WriteMsSqlColumn(TextWriter w, DbColumn col)
		{
			w.Write("[");
			w.Write(col.BaseColumnName);
			w.Write("] ");
			var type = col.DataType;
			var typeCode = Type.GetTypeCode(type);

			void WriteLength(TextWriter w, DbColumn col)
			{
				w.Write('(');
				if (col.IsLong == true || col.ColumnSize > 256)
				{
					w.Write("max");
				}
				else
				{
					var len = Math.Min(col.ColumnSize ?? 256, 256);
					w.Write(len);
				}
				w.Write(')');
			}

			switch (typeCode)
			{
				case TypeCode.Boolean:
					w.Write("bit");
					break;
				case TypeCode.Int32:
					w.Write("int");
					break;
				case TypeCode.Int64:
					w.Write("bigint");
					break;
				case TypeCode.String:
					w.Write("varchar");
					WriteLength(w, col);
					break;
				case TypeCode.DateTime:
					w.Write("datetime2");
					break;
				case TypeCode.Single:
					w.Write("float");
					break;
				case TypeCode.Double:
					w.Write("double");
					break;
				default:
					if (type == typeof(byte[]))
					{
						w.Write("varbinary");
						WriteLength(w, col);
					}
					if (type == typeof(Guid))
					{
						w.Write("uniqueidentifier");
					}
					throw new NotSupportedException();
			}
			if (col.AllowDBNull == true)
			{
				w.Write(" null");
			}
			else
			{
				w.Write(" not null");
			}
		}

		void WriteMsSqlSchema(TextWriter w, string tableName, ReadOnlyCollection<DbColumn> schema)
		{
			w.Write("create table ");
			w.Write("[" + tableName + "]");
			w.WriteLine();
			w.Write("(");

			bool first = true;
			foreach(var col in schema)
			{
				if(first)
				{
					first = false;
				} else
				{
					w.Write(',');
				}
				w.WriteLine();
				WriteMsSqlColumn(w, col);
			}
			w.WriteLine();

			w.Write(")");
			w.WriteLine();
		}

		string GetMsSqlCreateTable(DbDataReader data, string table)
		{
			var w = new StringWriter();
			WriteMsSqlSchema(w, table, data.GetColumnSchema());
			return w.ToString();
		}

		void WriteTo(SqlConnection conn, DbDataReader data, string tableName)
		{
			var tx = conn.BeginTransaction();
			var createCmd = GetMsSqlCreateTable(data, tableName);

			var cmd = conn.CreateCommand();
			cmd.CommandText = createCmd;
			cmd.ExecuteNonQuery();

			var bc = new SqlBulkCopy(conn, SqlBulkCopyOptions.CheckConstraints, tx);
			bc.DestinationTableName = tableName;
			bc.WriteToServer(data);
			tx.Commit();
		}

		void WriteTo(MySqlConnection conn, DbDataReader data)
		{
			throw new NotImplementedException();
		}

		void WriteTo(NpgsqlConnection conn, DbDataReader data)
		{
			throw new NotImplementedException();
		}

		static MySqlConnection GetMySqlConnection()
		{
			var connStr = Environment.GetEnvironmentVariable("MySqlConnStr");
			var csb = new MySqlConnectionStringBuilder(connStr);
			csb.Database = "floop";
			csb.AllowLoadLocalInfile = true;
			connStr = csb.ToString();
			var conn = new MySqlConnection(connStr);
			conn.Open();
			return conn;
		}

		static void SqlToMySql()
		{
			var csb = new SqlConnectionStringBuilder
			{
				DataSource = ".",
				InitialCatalog = "scdev",
				IntegratedSecurity = true,
				MultipleActiveResultSets = true,
			};

			using var conn = new SqlConnection(csb.ConnectionString);
			conn.Open();
			var cmd = conn.CreateCommand();

			cmd.CommandText = "select table_schema, table_name from information_schema.tables where table_type = 'BASE TABLE'";
			using var reader = cmd.ExecuteReader();

			var dc = conn.CreateCommand();

			var mconn = GetMySqlConnection();
			var mcmd = mconn.CreateCommand();

			var t1 = Stopwatch.StartNew();
			while (reader.Read())
			{
				var schema = reader.GetString(0);
				var tableName = reader.GetString(1);

				dc.CommandText = "select * from [" + schema + "].[" + tableName + "]";
				using var dr = dc.ExecuteReader();

				var sw = new StringWriter();
				CreateTable(sw, tableName, dr);

				var str = sw.ToString();
				mcmd.CommandText = str;
				mcmd.ExecuteNonQuery();

				Console.Write("writing " + tableName);

				var t2 = Stopwatch.StartNew();
				var bc = new MySqlBulkCopy(mconn);
				bc.BulkCopyTimeout = 0;
				bc.DestinationTableName = tableName;
				bc.WriteToServer(dr);
				t2.Stop();
				Console.WriteLine(t2.Elapsed.ToString());
			}
			t1.Stop();
			Console.WriteLine(t1.Elapsed.ToString());
		}

		static void CreateTable(TextWriter writer, string table, IDbColumnSchemaGenerator schema)
		{
			var cols = schema.GetColumnSchema();

			writer.WriteLine("create table " + table + "(");

			bool first = true;
			foreach (var col in cols)
			{
				if (first == true)
				{
					first = false;
				}
				else
				{
					writer.WriteLine(',');
				}

				writer.Write("  ");
				writer.Write(col.ColumnName);
				writer.Write(" ");
				switch (Type.GetTypeCode(col.DataType))
				{
					case TypeCode.Boolean:
						writer.Write("bit");
						break;
					case TypeCode.DateTime:
						writer.Write("datetime");
						break;
					case TypeCode.Byte:
						writer.Write("tinyint");
						break;
					case TypeCode.Int16:
						writer.Write("smallint");
						break;
					case TypeCode.Int32:
						writer.Write("int");
						break;
					case TypeCode.Int64:
						writer.Write("bigint");
						break;
					case TypeCode.String:
						if (col.IsLong == true)
						{
							writer.Write("text");
						}
						else
						{
							writer.Write("varchar");
							writer.Write('(');
							writer.Write(Math.Min(255, col.ColumnSize ?? 255));
							writer.Write(')');
						}
						break;
					case TypeCode.Object:
						if (col.DataType == typeof(byte[]))
						{
							if (col.IsLong == true)
							{
								writer.Write("blob");
							}
							else
							{
								if (IsFixed(col))
								{
									writer.Write("binary");
								}
								else
								{
									writer.Write("varbinary");
								}
								writer.Write('(');
								writer.Write(col.ColumnSize ?? 255);
								writer.Write(')');
							}
						}
						else
						{

						}
						break;
					default:

						break;
				}
				writer.Write(' ');
				if (col.AllowDBNull == false)
				{
					writer.Write("not null");
				}
				else
				{
					writer.Write("null");
				}

			}
			writer.WriteLine();
			writer.WriteLine(")");
			writer.WriteLine();
		}

		static bool IsFixed(DbColumn col)
		{
			return col.DataTypeName == "binary";
		}

		static void SqlToCsv()
		{
			var csb = new SqlConnectionStringBuilder
			{
				DataSource = ".",
				InitialCatalog = "scdev",
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

			//long progress = 0;

			long updateT = totalSize / 1000;

			//long pp = 0;
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
					//Action<long> cb = (r) =>
					//{
					//	progress += rowInc;
					//	pp += rowInc;
					//	if (pp > updateT)
					//	{
					//		pp -= updateT;

					//		var p = progress * 100.0 / totalSize;

					//		var e = sw.Elapsed;
					//		var total = e.Ticks * totalSize / progress;
					//		var time = TimeSpan.FromTicks(total);

					//		cc.SetCursorPosition(0, 2);
					//		cc.WriteLine($"                                                                                                          ");
					//		cc.SetCursorPosition(0, 2);
					//		cc.Write($"{p,5:0.0}% {(int)e.TotalSeconds}/{(int)time.TotalSeconds}");
					//	}
					//};
					var csvw = new CsvDataWriter(ts, opts);
					csvw.Write(dr);
				}
			}
		}
	}
}
