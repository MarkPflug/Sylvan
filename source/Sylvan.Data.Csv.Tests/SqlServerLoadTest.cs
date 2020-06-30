using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class SqlServerLoadTest
	{
		[Fact(Skip = "Sample")]
		public void BulkLoad()
		{
			using var conn = GetConnection();

			var dr = TestData.GetTestDataReader(1000000, 10);
			var targetTable = GetTestTableName("BulkLoadSimple");
			Insert(conn, targetTable, dr);
		}

		[Fact(Skip = "Sample")]
		public void BulkLoad2()
		{
			const string FileName = @"C:\users\mark\desktop\OP_DTL_GNRL_PGYR2018_P01172020.csv";

			var result = GetSchema(FileName);
			var schema = result.GetColumnSchema();

			using var conn = GetConnection();

			var targetTableName = GetTestTableName("GiantCSV");

			var csvSchema = new CsvSchema(schema);
			var opts = new CsvDataReaderOptions { Schema = csvSchema };
			using var tr = File.OpenText(FileName);
			DbDataReader dr = CsvDataReader.Create(tr, opts);
			//dr = new BoundedDataReader(dr, 10000);
			dr = new TruncatingDataReader(dr, 128);
			Insert(conn, targetTableName, dr);
		}

		static string GetTestTableName(string baseName)
		{
			return baseName + "_" + DateTime.UtcNow.ToString("yyyyMMddHHmmssfff");
		}

		static void WriteData(TextWriter writer, DbDataReader reader)
		{
			for (int i = 0; i < reader.FieldCount; i++)
			{
				if (i > 0)
					writer.Write("\t");
				writer.Write(reader.GetName(i));
			}
			writer.WriteLine();
			while (reader.Read())
			{
				for (int i = 0; i < reader.FieldCount; i++)
				{
					if (i > 0)
						writer.Write("\t");
					writer.Write(reader.GetValue(i)?.ToString());
				}
				writer.WriteLine();
			}
		}

		static SqlConnection GetConnection()
		{
			var csb = 
				new SqlConnectionStringBuilder {
					DataSource = @".",
					InitialCatalog = "Test",
					IntegratedSecurity = true
				};
			var connStr = csb.ToString();
			var conn = new SqlConnection(connStr);
			conn.Open();
			return conn;
		}


		static IDbColumnSchemaGenerator GetSchema(string file)
		{
			using var tr = File.OpenText(file);
			var dr = CsvDataReader.Create(tr);
			var a = new SchemaAnalyzer();
			var result = a.Analyze(dr);
			return result;
		}

		static void Insert(SqlConnection conn, string tableName, DbDataReader data)
		{
			var tableDef = BuildTable(tableName, (IDbColumnSchemaGenerator)data);
			var cmd = conn.CreateCommand();
			cmd.CommandText = tableDef;
			cmd.ExecuteNonQuery();

			var sw = Stopwatch.StartNew();
			var bc = new SqlBulkCopy(conn);
			bc.BulkCopyTimeout = 0;
			bc.DestinationTableName = tableName;
			bc.WriteToServer(data);
			sw.Stop();

			Console.WriteLine($"Inserted in {sw.Elapsed.ToString()}");
		}

		static string BuildTable(string name, IDbColumnSchemaGenerator schema)
		{
			var cols = schema.GetColumnSchema();

			var w = new StringWriter();

			w.WriteLine("create table " + name + " (");

			var first = true;
			foreach (var col in cols)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					w.WriteLine(",");
				}

				w.Write(col.ColumnName);
				w.Write(' ');
				switch (Type.GetTypeCode(col.DataType))
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
						if (col.IsLong == true)
						{
							w.Write("varchar(max)");
						}
						else
						{
							w.Write("varchar");
							var len = Math.Min(col.ColumnSize ?? 256, 65535);
							w.Write('(');
							w.Write(len);
							w.Write(')');
						}
						break;
					case TypeCode.DateTime:
						w.Write("datetime");
						break;
					case TypeCode.Single:
						w.Write("float");
						break;
					case TypeCode.Double:
						w.Write("double");
						break;
					default:
						throw new NotSupportedException();
				}

				w.Write(' ');
				w.Write(col.DataType != typeof(string) && col.AllowDBNull == false ? " not null" : "null");
			}
			w.WriteLine();
			w.WriteLine(");");
			return w.ToString();
		}
	}
}
