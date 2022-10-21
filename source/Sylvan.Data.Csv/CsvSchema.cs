using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Sylvan.Data.Csv;

sealed class NullableCsvSchema : CsvSchemaProvider
{
	static NullableStringColumn Column = new();

	public override DbColumn? GetColumn(string? name, int ordinal)
	{
		return Column;
	}

	class NullableStringColumn : DbColumn
	{
		public NullableStringColumn()
		{
			this.AllowDBNull = true;
		}
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

	readonly DbColumn[] schema;
	readonly Dictionary<string, DbColumn> nameMap;

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
		this.nameMap = new Dictionary<string, DbColumn>(headerComparer);
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
				nameMap.Add(col.BaseColumnName, col);
			}
			else
			if (string.IsNullOrEmpty(col.ColumnName) == false)
			{
				nameMap.Add(col.ColumnName, col);
			}
		}
	}

	/// <inheritdoc />
	public override DbColumn? GetColumn(string? name, int ordinal)
	{
		if (name != null && nameMap.TryGetValue(name, out DbColumn? col))
		{
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
