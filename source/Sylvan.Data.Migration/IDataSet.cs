using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data.Common;

namespace Sylvan.Data.Migration
{
	interface IDataTable
	{
		string Name { get; }
		DbDataReader GetReader();
	}

	interface IDataSet
	{
		IEnumerable<IDataTable> Tables { get; }
	}

	interface IDataSetFactory
	{
		IDataSet Create(string connectionString);
	}


	class SqlClientDataSet : IDataSet
	{

		class SqlClientDataTable : IDataTable
		{
			SqlConnection conn;
			string tableSchema;
			string tableName;

			public SqlClientDataTable(SqlConnection conn, string tableSchema, string tableName)
			{
				this.conn = conn;
				this.tableSchema = tableSchema;
				this.tableName = tableName;
			}

			public string Name => tableName;

			public DbDataReader GetReader()
			{
				var cmd = conn.CreateCommand();
				cmd.CommandText = "select * from \"" + tableSchema + "\".\"" + tableName + "\"";
				return cmd.ExecuteReader();
			}
		}


		SqlConnection conn;

		public SqlClientDataSet(SqlConnection conn)
		{
			this.conn = conn;
		}

		public IEnumerable<IDataTable> Tables
		{
			get
			{
				var cmd = conn.CreateCommand();
				cmd.CommandText = "select table_schema, table_name from INFORMATION_SCHEMA.TABLES";
				var reader = cmd.ExecuteReader();
				while (reader.Read())
				{
					yield return new SqlClientDataTable(this.conn, reader.GetString(0), reader.GetString(1));
				}
			}
		}
	}
}
