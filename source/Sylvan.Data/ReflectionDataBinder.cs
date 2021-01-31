using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace Sylvan.Data
{
	// A DataBinder implementatin that uses reflection.
	// This was created merely to compare performance with CompiledDataBinder.
	sealed class ReflectionDataBinder<T> : IDataBinder<T>
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
				schema
				.Where(c => !string.IsNullOrEmpty(c.ColumnName))
				.Select((c, i) => new { Column = c, Idx = c.ColumnOrdinal ?? throw new ArgumentException() })
				.ToDictionary(p => p.Column.ColumnName, p => new { p.Column, p.Idx });

			DbColumn? GetCol(int? idx, string? name)
			{
				if (!string.IsNullOrEmpty(name))
				{
					// interesting that this needs to be annoted with not-null
					if (ordinalMap!.TryGetValue(name!, out var c))
					{
						return c.Column;
					}
				}
				if (idx != null)
				{
					return schema[idx.Value];
				}
				return null;
			}

			var propBinderList = new List<Action<IDataRecord, T>>();

			foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
			{
				args[0] = null;

				var dataMemberAttr = property.GetCustomAttribute<DataMemberAttribute>();
				var columnOrdinal = dataMemberAttr?.Order;
				var columnName = dataMemberAttr?.Name ?? property.Name;

				var setter = property.GetSetMethod(true)!;

				var paramType = setter.GetParameters()[0].ParameterType;

				var col = GetCol(columnOrdinal, columnName);
				if (col == null)
				{
					// TODO: potentially add an argument to throw if there is an unbound property?
					continue;
				}

				var ordinal = col.ColumnOrdinal ?? columnOrdinal ?? -1;

				if (ordinal < 0)
				{
					// this means the column didn't know it's own ordinal, and neither did the property.
					continue;
				}

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

		void IDataBinder<T>.Bind(IDataRecord record, T item)
		{
			foreach (var pb in propBinders)
			{
				pb(record, item);
			}
		}
	}
}
