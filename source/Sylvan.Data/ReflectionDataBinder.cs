using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Sylvan.Data
{
	// A DataBinder implementatin that uses reflection.
	// This was created merely to compare performance with CompiledDataBinder.
	sealed class ReflectionDataBinder<T> : DataBinder<T>
	{
		readonly ReadOnlyCollection<DbColumn> schema;
		readonly DbColumn[] columns;

		readonly object?[] args = new object?[1];
		readonly Type type;

		readonly Action<IDataRecord, T>[] propBinders;

		public ReflectionDataBinder(ReadOnlyCollection<DbColumn> schema)
		{
			this.type = typeof(T);
			this.schema = schema;

			this.columns = schema.ToArray();
			var ordinalMap =
				this.columns
				.Where(c => !string.IsNullOrEmpty(c.ColumnName))
				.ToDictionary(c => c.ColumnName, c => c.ColumnOrdinal ?? -1);

			var propBinderList = new List<Action<IDataRecord, T>>();

			foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
			{
				args[0] = null;
				var columnOrdinal = property.GetCustomAttribute<ColumnOrdinalAttribute>()?.Ordinal;
				var columnName = property.GetCustomAttribute<ColumnNameAttribute>()?.Name ?? property.Name;

				var setter = property.GetSetMethod(true);

				var paramType = setter.GetParameters()[0].ParameterType;
				var ordinal =
					columnName != null
					? (ordinalMap.TryGetValue(columnName, out int o) ? o : -1)
					: columnOrdinal ?? -1;
				if (ordinal == -1) throw new Exception("Binding failure");

				var col = columns[ordinal];


				var type = col.DataType;
				var typeCode = Type.GetTypeCode(type);
				Func<IDataRecord, object> selector;

				switch (typeCode)
				{
					case TypeCode.Int32:
						selector = r => r.GetInt32(ordinal);
						break;
					case TypeCode.DateTime:
						selector = r => r.GetDateTime(ordinal);
						break;
					case TypeCode.String:
						selector = r => r.GetString(ordinal);
						break;
					case TypeCode.Double:
						selector = r => r.GetDouble(ordinal);
						break;
					default:
						if (col.DataType == typeof(Guid))
						{
							selector = r => r.GetGuid(ordinal);
							break;
						}
						continue;
				}

				Action<IDataRecord, T>? propBinder = null;
				if (col.AllowDBNull != false)
				{
					propBinder = (r, i) =>
					{
						if (r.IsDBNull(ordinal) == false)
						{
							var val = selector(r);
							args[0] = val;
							setter.Invoke(i, args);
						}
					};
				}
				else
				{
					propBinder = (r, i) =>
					{
						var val = selector(r);
						args[0] = val;
						setter.Invoke(i, args);
					};
				}
				propBinderList.Add(propBinder);
			}
			this.propBinders = propBinderList.ToArray();
		}

		public override void Bind(IDataRecord record, T item)
		{
			foreach (var pb in propBinders)
			{
				pb(record, item);
			}
		}
	}
}
