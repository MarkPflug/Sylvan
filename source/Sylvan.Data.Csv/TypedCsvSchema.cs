using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Sylvan.Data.Csv
{
	public class TypedCsvColumn : DbColumn
	{
		public TypedCsvColumn(Type type, bool allowNull)
		{
			this.DataType = type;
			this.AllowDBNull = allowNull;
		}
	}

	public class TypedCsvSchema : ICsvSchemaProvider
	{
		Dictionary<string, Type> nameMap;
		Dictionary<int, Type> ordinalMap;

		public TypedCsvSchema()
		{
			this.nameMap = new Dictionary<string, Type>();
			this.ordinalMap = new Dictionary<int, Type>();
		}

		public void Add(string column, Type type)
		{
			this.nameMap.Add(column, type);
		}

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
