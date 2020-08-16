using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
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

	public abstract class DataBinder<T> where T : class, new()
	{

		public T Bind(IDataRecord record)
		{
			var t = new T();
			Bind(record, t);
			return t;
		}

		public abstract void Bind(IDataRecord record, T item);

	}

	public sealed class CompiledDataBinder<T> : DataBinder<T> where T : class, new()
	{
		Action<IDataRecord, T> f;

		public CompiledDataBinder(ReadOnlyCollection<DbColumn> schema)
		{

			var ordinalMap =
				schema
				.Where(c => !string.IsNullOrEmpty(c.ColumnName))
				.Select((c, i) => new { Column = c, Idx = i })
				.ToDictionary(p => p.Column.ColumnName, p => (p.Column, p.Idx));

			var drType = typeof(IDataRecord);

			var targetType = typeof(T);

			var recordParam = Expression.Parameter(drType);
			var itemParam = Expression.Parameter(targetType);
			var localsMap = new Dictionary<Type, ParameterExpression>();
			var locals = new List<ParameterExpression>();

			var bodyExpressions = new List<Expression>();

			//var targetLocal = Expression.Variable(targetType);
			//locals.Add(targetLocal);
			//var init = Expression.Assign(targetLocal, Expression.New(targetType.GetConstructor(Array.Empty<Type>())));
			//bodyExpressions.Add(targetLocal);
			//bodyExpressions.Add(init);

			var isDbNullMethod = drType.GetMethod("IsDBNull");

			var type = typeof(T);

			foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
			{
				DbColumn col;

				var columnOrdinal = property.GetCustomAttribute<ColumnOrdinalAttribute>()?.Ordinal;
				var columnName = property.GetCustomAttribute<ColumnNameAttribute>()?.Name ?? property.Name;
				(DbColumn c, int idx) colInfo;
				if (!ordinalMap.TryGetValue(columnName, out colInfo))
				{
					continue;
				}
				col = colInfo.c;
				var ordinal = col.ColumnOrdinal ?? colInfo.idx;

				if (ordinal == -1) continue;

				var setter = property.GetSetMethod(true);
				var paramType = setter.GetParameters()[0].ParameterType;

				MethodInfo getter;

				switch (Type.GetTypeCode(col.DataType))
				{
					case TypeCode.Int32:
						getter = drType.GetMethod("GetInt32");
						break;
					case TypeCode.DateTime:
						getter = drType.GetMethod("GetDateTime");
						break;
					case TypeCode.String:
						getter = drType.GetMethod("GetString");
						break;
					case TypeCode.Double:
						getter = drType.GetMethod("GetDouble");
						break;
					default:
						if (col.DataType == typeof(Guid))
						{
							getter = drType.GetMethod("GetGuid");
							break;
						}
						continue;
				}

				var ordinalExpr = Expression.Constant(ordinal, typeof(int));
				Expression expr;

				if (col.AllowDBNull != false)
				{
					expr =
						Expression.IfThen(
							Expression.Not(Expression.Call(recordParam, isDbNullMethod, ordinalExpr)),
							Expression.Call(
								itemParam, 
								setter, 
								Expression.New(
									paramType.GetConstructor(
										new Type[] { paramType.GetGenericArguments()[0] }
									), 
									Expression.Call(recordParam, getter, ordinalExpr)
								)
							)
						);
				}
				else
				{
					expr = Expression.Call(itemParam, setter, Expression.Call(recordParam, getter, ordinalExpr));
				}
				bodyExpressions.Add(expr);
			}

			var body = Expression.Block(locals, bodyExpressions);

			var lf = Expression.Lambda<Action<IDataRecord, T>>(body, recordParam, itemParam);
			this.f = lf.Compile();
		}

		public override void Bind(IDataRecord record, T item)
		{
			f(record, item);
		}
	}

	public sealed class ReflectionDataBinder<T> : DataBinder<T> where T : class, new()
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
