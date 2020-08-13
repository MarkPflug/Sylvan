using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;

namespace Sylvan.Data
{
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public sealed class ColumnOrdinalAttribute : Attribute
	{
		public int Ordinal { get; }
		public ColumnOrdinalAttribute(int ordinal)
		{
			this.Ordinal = ordinal;
		}
	}

	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public sealed class ColumnNameAttribute : Attribute
	{
		public string Name { get; }
		public ColumnNameAttribute(string name)
		{
			this.Name = name;
		}
	}

	public sealed class DataBinder<T> where T : class, new()
	{
		readonly ReadOnlyCollection<DbColumn> schema;
		readonly DbColumn[] columns;

		public DataBinder(ReadOnlyCollection<DbColumn> schema)
		{
			this.schema = schema;

			// or just schema.ToArray()?
			this.columns =
				Enumerable
				.Range(0, schema.Count)
				.Select(i => schema.FirstOrDefault(c => c.ColumnOrdinal == i))
				.ToArray();
			
		}

		public T Bind(IDataRecord record)
		{
			var item = new T();

			var type = typeof(T);

			var args = new object?[1];
			foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
			{
				args[0] = null;
				var columnOrdinal = property.GetCustomAttribute<ColumnOrdinalAttribute>()?.Ordinal;
				var columnName = property.GetCustomAttribute<ColumnNameAttribute>()?.Name ?? property.Name;

				var setter = property.GetSetMethod(true);

				var paramType = setter.GetParameters()[0].ParameterType;
				var ordinal =
					columnName != null
					? record.GetOrdinal(columnName)
					: columnOrdinal ?? -1;
				if (ordinal == -1) throw new Exception("Binding failure");

				var col = columns[ordinal];

				var isNull = col.AllowDBNull != false && record.IsDBNull(ordinal);

				if (!isNull)
				{
					args[0] = record.GetValue(ordinal);
					setter.Invoke(item,  args);
				}
			}

			return item;
		}
	}
}
