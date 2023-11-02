using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Sylvan.Data.Csv;

sealed class NullableCsvSchema : CsvSchemaProvider
{
	readonly static SchemaColumn NullableStringColumn = new(typeof(string), true);

	public override DbColumn? GetColumn(string? name, int ordinal)
	{
		return NullableStringColumn;
	}
}

sealed class DynamicCsvSchema : CsvSchemaProvider
{
	readonly static SchemaColumn DynamicColumn = new(typeof(object), true);

	public override DbColumn? GetColumn(string? name, int ordinal)
	{
		return DynamicColumn;
	}
}

sealed class SchemaColumn : DbColumn
{
	public SchemaColumn(Type type, bool allowNull)
	{
		this.DataType = type;
		this.AllowDBNull = allowNull;
	}
}

/// <summary>
/// An ICsvSchemaProvider implementation based on an existing schema.
/// </summary>
public class CsvSchema : CsvSchemaProvider
{
	/// <summary>
	/// Gets a ICsvSchemaProvider that treats empty strings as null.
	/// </summary>
	// TODO: this probably should have been a readonly property.
	public static readonly ICsvSchemaProvider Nullable = new NullableCsvSchema();

	/// <summary>
	/// Gets a ICsvSchemaProvider that treats fields as "dynamic", where
	/// the data type can change from row to row.
	/// </summary>
	public static readonly ICsvSchemaProvider Dynamic = new DynamicCsvSchema();

	readonly DbColumn[] schema;
	readonly Dictionary<string, DbColumn?> nameMap;

	const string SeriesOrdinalProperty = "SeriesOrdinal";
	readonly DbColumn? seriesColumn;

	/// <summary>
	/// Creates a new CsvSchemaProvider.
	/// </summary>
	/// <param name="schema">An DbColumn schema.</param>
	public CsvSchema(IEnumerable<DbColumn> schema)
		: this(schema, StringComparer.OrdinalIgnoreCase)
	{
	}

	/// <summary>
	/// Creates a new CsvSchemaProvider.
	/// </summary>
	/// <param name="schema">An DbColumn schema.</param>
	/// <param name="headerComparer">A StringComparer used to match header names to the provided schema.</param>
	public CsvSchema(IEnumerable<DbColumn> schema, StringComparer headerComparer)
	{
		this.schema = schema.ToArray();
		this.nameMap = new Dictionary<string, DbColumn?>(headerComparer);
		foreach (var col in schema)
		{
			int? series = col[SeriesOrdinalProperty] is int i ? i : (int?)null;
			if (series.HasValue)
			{
				if (this.seriesColumn != null)
				{
					// Only a single series column is supported.
					throw new NotSupportedException();
				}
				this.seriesColumn = col;
				continue;
			}

			if (string.IsNullOrEmpty(col.BaseColumnName) == false)
			{
				var key = col.BaseColumnName;
				if (nameMap.ContainsKey(key))
				{
					nameMap[key] = null;
				}
				else
				{
					nameMap.Add(key, col);
				}
			}
			else
			if (string.IsNullOrEmpty(col.ColumnName) == false)
			{
				var key = col.ColumnName;
				if (nameMap.ContainsKey(key))
				{
					nameMap[key] = null;
				}
				else
				{
					nameMap.Add(key, col);
				}
			}
		}
	}

	/// <inheritdoc />
	public override DbColumn? GetColumn(string? name, int ordinal)
	{
		if (name != null && nameMap.TryGetValue(name, out DbColumn? col))
		{
			// this can still return null when the column name was duplicated
			// and thus ambiguous
			return col;
		}

		if (seriesColumn != null)
		{
			return seriesColumn;
		}

		if (ordinal >= 0 && ordinal < schema.Length)
		{
			col = schema[ordinal];
			if (col.BaseColumnName == null)
				return col;
		}

		return null;
	}
}
