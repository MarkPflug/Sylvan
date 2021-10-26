using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Sylvan.Data
{
	/// <summary>
	/// Provides schema information for data.
	/// </summary>
	public sealed partial class Schema : IReadOnlyList<Schema.Column>, IDbColumnSchemaGenerator
	{
		static T? GetValue<T>(DataRow row, string name)
		{
			if (row.Table.Columns.Contains(name))
			{
				object obj = row[name];
				if (obj is T)
				{
					return (T)obj;
				}
			}
			return default;
		}

		internal static Schema FromSchemaTable(DataTable dt)
		{
			var builder = 
				new Builder();

			foreach(DataRow? row in dt.Rows)
			{
				if (row == null) continue;

				var cb = new Column.Builder();
				cb.AllowDBNull = GetValue<bool?>(row, SchemaTableColumn.AllowDBNull);
				cb.ColumnName = GetValue<string>(row, SchemaTableColumn.ColumnName) ?? string.Empty;
				cb.ColumnOrdinal = GetValue<int?>(row, SchemaTableColumn.ColumnOrdinal);
				cb.DataType = GetValue<Type>(row, SchemaTableColumn.DataType);
				builder.Add(cb);				
			}

			return builder.Build();			
		}

		internal static Schema GetWeakSchema(IDataReader dr)
		{
			var builder = new Builder();

			for(int i = 0; i < dr.FieldCount; i++)
			{
				var name = dr.GetName(i);
				var type = dr.GetFieldType(i);
				
				var cb = new Column.Builder();
				// without a schema, have to assume can be null.
				cb.AllowDBNull = true;
				cb.ColumnName = name ?? string.Empty;
				cb.ColumnOrdinal = i;
				cb.DataType = type ?? typeof(object);
				builder.Add(cb);
			}

			return builder.Build();
		}

		// types: byte,int16,int32,int64,float,double,decimal,string,binary,date,datetime,
		// Id:int;
		// FirstName:string[32]?;
		// LastName:string[32]?;
		// *:double?;
		Column[] columns;

		public int Count => columns.Length;

		public Column this[int index] => columns[index];

		private Schema(IEnumerable<Column> cols)
		{
			this.columns = cols.ToArray();
		}

		public static implicit operator ReadOnlyCollection<DbColumn>(Schema schema)
		{
			return new ReadOnlyCollection<DbColumn>(schema.columns);
		}

		/// <summary>
		/// Creates a Schema from the schema of an existing data reader.
		/// </summary>
		/// <param name="dataReader">The data reader to use as a schema template.</param>
		public Schema(DbDataReader dataReader) : this(dataReader.GetColumnSchema()) { }

		/// <summary>
		/// Creates a Schema from an existing schema.
		/// </summary>
		/// <param name="schema">The schema to use as a template.</param>
		public Schema(ReadOnlyCollection<DbColumn> schema)
		{
			this.columns =
				schema
				.Select(c => c as Column ?? new Column.Builder(c).Build())
				.ToArray();
		}

		/// <inheritdoc/>
		public override string ToString()
		{
			return SimpleSchemaSerializer.SingleLine.GetSchemaSpec(this);
		}

		public static Schema Parse(string spec)
		{
			return SimpleSchemaSerializer.SingleLine.Parse(spec);
		}

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			return new ReadOnlyCollection<DbColumn>(this.columns);
		}

		public IEnumerator<Column> GetEnumerator()
		{
			return this.columns.AsEnumerable().GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}
	}
}
