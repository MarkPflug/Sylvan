using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Sylvan.Data.Csv
{
	class TypedCsvColumn : DbColumn
	{
		public TypedCsvColumn(Type type, bool allowNull)
		{
			this.DataType = type;
			this.AllowDBNull = allowNull;
		}
	}

	/// <summary>
	/// An implementation of ICsvSchemaProvider that allows specifying a data type for columns.
	/// </summary>
	public class TypedCsvSchema : ICsvSchemaProvider
	{
		Dictionary<string, Type> nameMap;
		Dictionary<int, Type> ordinalMap;

		/// <summary>
		/// Creates a new TypedCsvSchema.
		/// </summary>
		public TypedCsvSchema()
		{
			this.nameMap = new Dictionary<string, Type>();
			this.ordinalMap = new Dictionary<int, Type>();
		}

		/// <summary>
		/// Specifies a type for the column with a given name.
		/// </summary>
		/// <param name="column">The column name.</param>
		/// <param name="type">The type of the column.</param>
		public void Add(string column, Type type)
		{
			this.nameMap.Add(column, type);
		}

		/// <summary>
		/// Specifies a type for the column with a given ordinal.
		/// </summary>
		/// <param name="ordinal">The column ordinal.</param>
		/// <param name="type">The type of the column.</param>
		public void Add(int ordinal, Type type)
		{
			this.ordinalMap.Add(ordinal, type);
		}

		DbColumn? ICsvSchemaProvider.GetColumn(string? name, int ordinal)
		{
			Type type;
			if ((name != null && nameMap.TryGetValue(name, out type)) || ordinalMap.TryGetValue(ordinal, out type))
			{
				bool allowNull = type != typeof(string);
				if(type.IsGenericType)
				{
					if(type.GetGenericTypeDefinition() == typeof(Nullable<>))
					{
						type = type.GetGenericArguments()[0];
						allowNull = true;
					}
				}
				
				return new TypedCsvColumn(type, allowNull);
			}
			return null;
		}
	}
}
