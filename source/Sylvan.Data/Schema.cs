using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Sylvan.Data;

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
			if (obj is T item)
			{
				return item;
			}
		}
		return default;
	}

	/// <summary>
	/// Gets a Schema instance defining the schema from schemaTable.
	/// </summary>
	/// <param name="schemaTable">A DataTable returned from a <see cref="IDataReader.GetSchemaTable()"/></param>
	/// <returns>A schema instance.</returns>
	public static Schema FromSchemaTable(DataTable schemaTable)
	{
		var builder =
			new Builder();

		foreach (DataRow? row in schemaTable.Rows)
		{
			if (row == null) continue;

			var cb = new Column.Builder
			{
				AllowDBNull = GetValue<bool?>(row, SchemaTableColumn.AllowDBNull),
				ColumnName = GetValue<string>(row, SchemaTableColumn.ColumnName) ?? string.Empty,
				ColumnOrdinal = GetValue<int?>(row, SchemaTableColumn.ColumnOrdinal),
				DataType = GetValue<Type>(row, SchemaTableColumn.DataType)
			};
			builder.Add(cb);
		}

		return builder.Build();
	}

	internal static Schema GetWeakSchema(IDataReader dr)
	{
		var builder = new Builder();

		for (int i = 0; i < dr.FieldCount; i++)
		{
			var name = dr.GetName(i);
			var type = dr.GetFieldType(i);

			var cb = new Column.Builder
			{
				// without a schema, have to assume can be null.
				AllowDBNull = true,
				ColumnName = name ?? string.Empty,
				ColumnOrdinal = i,
				DataType = type ?? typeof(object)
			};
			builder.Add(cb);
		}

		return builder.Build();
	}

	// types: byte,int16,int32,int64,float,double,decimal,string,binary,date,datetime,
	// Id:int;
	// FirstName:string[32]?;
	// LastName:string[32]?;
	// *:double?;
	readonly Column[] columns;

	/// <summary>
	/// Gets the number of columns in the schema.
	/// </summary>
	public int Count => columns.Length;

	/// <summary>
	/// Gets the column at the given index.
	/// </summary>
	public Column this[int index] => columns[index];

	private Schema(IEnumerable<Column> cols)
	{
		this.columns = cols.ToArray();
	}

	/// <summary>
	/// Converts the schema to a <see cref="ReadOnlyCollection{DbColumn}"/>.
	/// </summary>
	/// <param name="schema"></param>
	public static implicit operator ReadOnlyCollection<DbColumn>(Schema schema)
	{
		return schema.GetColumnSchema();
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

	/// <summary>
	/// Parses the specification string into a Schema object.
	/// </summary>
	public static Schema Parse(string spec)
	{
		return SimpleSchemaSerializer.Parse(spec);
	}

	/// <summary>
	/// Gets a column schema representing the current schema.
	/// </summary>
	public ReadOnlyCollection<DbColumn> GetColumnSchema()
	{
		return new ReadOnlyCollection<DbColumn>(this.columns);
	}

	/// <summary>
	/// Gets an enumerator of the schema column.
	/// </summary>
	public IEnumerator<Column> GetEnumerator()
	{
		return this.columns.AsEnumerable().GetEnumerator();
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return this.GetEnumerator();
	}
}
