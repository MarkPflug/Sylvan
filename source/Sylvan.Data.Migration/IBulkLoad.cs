using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Sylvan.Data.Migration
{
	public abstract class DataMigrationFactory
	{
		public abstract IBulkCopy CreateBulkCopy(DbConnection conn);

	}

	public interface IBulkCopy : IDisposable
	{
		string DestinationTableName { get; set; }

		void WriteToServer(DbDataReader data);
		Task WriteToServerAsync(DbDataReader data);
	}

	class SqlDataMigrationFactory : DataMigrationFactory
	{
		public override IBulkCopy CreateBulkCopy(DbConnection conn)
		{
			var bc = new SqlBulkCopy((SqlConnection)conn);
			bc.EnableStreaming = true;
			return new SqlClientBulkCopy(bc);
		}
	}

	class SqlClientBulkCopy : IBulkCopy
	{
		SqlBulkCopy bc;

		public SqlClientBulkCopy(SqlBulkCopy bc)
		{
			this.bc = bc;
		}
		public string DestinationTableName { 
			get => bc.DestinationTableName; 
			set => this.bc.DestinationTableName = value; 
		}

		public void Dispose()
		{
			((IDisposable)bc).Dispose();
		}

		public void WriteToServer(DbDataReader data)
		{
			bc.WriteToServer(data);
		}

		public Task WriteToServerAsync(DbDataReader data)
		{
			return bc.WriteToServerAsync(data);
		}
	}
}
