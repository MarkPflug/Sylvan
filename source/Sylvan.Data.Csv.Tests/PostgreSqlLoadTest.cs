using System;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using Npgsql;
using Xunit;



namespace Sylvan.Data.Csv
{
	public class PostreSqlLoadTest
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
			//dr = new BoundedDataReader(dr, 1000000);
			Insert(conn, targetTableName, dr);
		}

		static string GetTestTableName(string baseName)
		{
			return baseName + "_" + Guid.NewGuid().ToString("n");
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

		static NpgsqlConnection GetConnection()
		{
			var connStr = Environment.GetEnvironmentVariable("PostgreSqlConnStr");

			var csb = new NpgsqlConnectionStringBuilder(connStr) { Database = "Test" };
			var conn = new NpgsqlConnection(csb.ConnectionString);
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

		static void Insert(NpgsqlConnection conn, string tableName, DbDataReader data)
		{
			var tableDef = BuildTable(tableName, (IDbColumnSchemaGenerator)data);
			var cmd = conn.CreateCommand();
			cmd.CommandText = tableDef;
			cmd.ExecuteNonQuery();

			var sw = Stopwatch.StartNew();

#warning injection
			var i = conn.BeginBinaryImport("copy " + tableName + " from stdin with binary");

			object[] row = new object[data.FieldCount];
			int num = 0;
			try
			{
				while (data.Read())
				{

					num++;
					data.GetValues(row);
					for(int x = 0; x < row.Length; x++)
					{
						if(row[x] is string s && s.Length > 128)
						{
							row[x] = s.Substring(0, 128);
						}
					}
					i.WriteRow(row);
				}
				i.Complete();
				i.Close();
				i.Dispose();

			} catch(Exception)
			{
				Console.WriteLine("row: " + num);
				foreach(var item in row)
				{
					Console.WriteLine(item?.ToString());
					Console.WriteLine();
				}
			}

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
						w.Write("boolean");
						break;
					case TypeCode.Int32:
						w.Write("integer");
						break;
					case TypeCode.Int64:
						w.Write("bigint");
						break;
					case TypeCode.String:
						if (col.IsLong == true)
						{
							w.Write("text");
						}
						else
						{
							w.Write("varchar");
							var len = Math.Min(col.ColumnSize ?? 255, 65535);
							w.Write('(');
							w.Write(len);
							w.Write(')');
						}
						break;
					case TypeCode.DateTime:
						w.Write("timestamp");
						break;
					case TypeCode.Single:
						w.Write("real");
						break;
					case TypeCode.Double:
						w.Write("double");
						break;
					default:
						throw new NotSupportedException();
				}

				w.Write(' ');
				w.Write(col.AllowDBNull == false ? " not null" : "null");
			}
			w.WriteLine();
			w.WriteLine(");");
			return w.ToString();
		}
	}
}
