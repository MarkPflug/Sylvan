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

		public abstract T Bind(IDataRecord record);

	}

	public sealed class CompiledDataBinder<T> : DataBinder<T> where T : class, new()
	{
		Func<IDataRecord, T> f;
		public CompiledDataBinder(ReadOnlyCollection<DbColumn> schema) {

			var ordinalMap =
				schema
				.Where(c => !string.IsNullOrEmpty(c.ColumnName))
				.Select((c, i) => new { Column = c, Idx = i })
				.ToDictionary(p => p.Column.ColumnName, p => (p.Column, p.Idx));

			var drType = typeof(IDataRecord);

			var targetType = typeof(T);

			//var method = new DynamicMethod("Bind", targetType, new Type[] { drType }, true);
			var param = Expression.Parameter(drType);
			var localsMap = new Dictionary<Type, ParameterExpression>();
			var locals = new List<ParameterExpression>();

			var bodyExpressions = new List<Expression>();

			var targetLocal = Expression.Variable(targetType);
			locals.Add(targetLocal);
			var init = Expression.Assign(targetLocal, Expression.New(targetType.GetConstructor(Array.Empty<Type>())));
			bodyExpressions.Add(targetLocal);
			bodyExpressions.Add(init);

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

				//if (!localsMap.TryGetValue(paramType, out var local))
				//{
				//	local = Expression.Variable(paramType);
				//	localsMap.Add(paramType, local);
				//}

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
						if(col.DataType == typeof(Guid))
						{
							getter = drType.GetMethod("GetGuid");
							break;
						}
						continue;
				}

				var ordinalExpr = Expression.Constant(ordinal, typeof(int));
				Expression expr;
				var assign = Expression.Call(targetLocal, setter, Expression.Call(param, getter, ordinalExpr));

				if (col.AllowDBNull != false)
				{
					expr =
						Expression.IfThen(
							Expression.Not(Expression.Call(param, isDbNullMethod, ordinalExpr)),
							assign
						);
				}
				else
				{
					expr = assign;
				}
				bodyExpressions.Add(expr);
			}
			bodyExpressions.Add(targetLocal);

			var body = Expression.Block(locals, bodyExpressions);

			var lf = Expression.Lambda<Func<IDataRecord, T>>(body, param);
			this.f = lf.Compile();
		}

		public override T Bind(IDataRecord record)
		{
			return f(record);
		}
	}

	public sealed  class ReflectionDataBinder<T> : DataBinder<T> where T : class, new()
	{ 
		readonly ReadOnlyCollection<DbColumn> schema;
		readonly DbColumn[] columns;

		public ReflectionDataBinder(ReadOnlyCollection<DbColumn> schema)
		{
			this.schema = schema;

			// or just schema.ToArray()?
			this.columns =
				Enumerable
				.Range(0, schema.Count)
				.Select(i => schema.FirstOrDefault(c => c.ColumnOrdinal == i))
				.ToArray();
			
		}

		public override T Bind(IDataRecord record)
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
