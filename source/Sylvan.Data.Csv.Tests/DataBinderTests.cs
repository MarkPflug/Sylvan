using Dapper;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class DataBinderTests
	{

		class Forza
		{
			public int Year { get; set; }
			public string Manufacturer { get; set; }
			public string Model { get; set; }
			public int? FM7 { get; set; }
			public int? FH4 { get; set; }
		}

		const string SchemaSpec = "Year:int,Manufacturer,Model,FM7:int?,FH4:int?";

		ICsvSchemaProvider schema;
		CsvDataReaderOptions opts;
		string dataText;
		IDataBinder<Forza> binder;
		Func<IDataReader, Forza> dbinder;

		public DataBinderTests()
		{
			//var schema = Schema.TryParse("B:Boolean,D:DateTime,V:Double,G:Guid,I:Int32,S:String");
			//this.record = new TestRecord();
			//this.item = new Record();
			//this.compiled = new CompiledDataBinder<Record>(schema.GetColumnSchema());
			//this.reflection = new ReflectionDataBinder<Record>(schema.GetColumnSchema());

			var tr = File.OpenText(@"C:\Users\Mark\Downloads\data\forza7carordinals.csv");
			tr.ReadLine(); // 
			this.dataText = tr.ReadToEnd();
			this.schema = new CsvSchema(Schema.TryParse(SchemaSpec).GetColumnSchema());
			opts = new CsvDataReaderOptions { Schema = schema };

			var data = GetData();
			this.binder = DataBinder<Forza>.Create(data.GetColumnSchema());
			this.dbinder = data.GetRowParser<Forza>();

		}

		DbDataReader GetData()
		{
			var data = CsvDataReader.Create(new StringReader(dataText), opts);
			return data;
		}

		const int Count = 1000;

		[Fact]
		public void Compiled()
		{
			var data = GetData();
			while (data.Read())
			{
				var f = new Forza();
				binder.Bind(data, f);
			}
		}

		[Fact]
		public void Load()
		{
			var data = GetData();
			using var c = new SqlConnection(new SqlConnectionStringBuilder { DataSource = ".", InitialCatalog = "Sylvan", IntegratedSecurity = true }.ConnectionString);
			c.Open();
			var bcp = new SqlBulkCopy(c);
			bcp.DestinationTableName = "Forza";
			bcp.WriteToServer(data);

		}
	}
}
