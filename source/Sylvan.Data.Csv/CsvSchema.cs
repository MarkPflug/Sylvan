using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// A ICsvSchemaProvider implementation based on an existing schema.
	/// </summary>
	public class CsvSchema : ICsvSchemaProvider
	{
		readonly DbColumn[] schema;
		readonly Dictionary<string, DbColumn> nameMap;

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
		}

		/// <inheritdoc />
		public virtual DbColumn? GetColumn(string? name, int ordinal)
		{
			if (name != null && nameMap.TryGetValue(name, out var col))
				return col;
			if (ordinal >= 0 && ordinal < schema.Length)
				return schema[ordinal];
			return null;
		}
	}	
}
