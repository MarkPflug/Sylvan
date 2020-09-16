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
	sealed class CompiledDataBinder<T> : IDataBinder<T>
	{
		Action<IDataRecord, T> f;

		static readonly Type drType = typeof(IDataRecord);

		static MethodInfo GetAccessorMethod(Type type)
		{
			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return drType.GetMethod("GetBoolean");
				case TypeCode.Byte:
					return drType.GetMethod("GetByte");
				case TypeCode.Int16:
					return drType.GetMethod("GetInt16");
				case TypeCode.Int32:
					return drType.GetMethod("GetInt32");
				case TypeCode.Int64:
					return drType.GetMethod("GetInt64");
				case TypeCode.DateTime:
					return drType.GetMethod("GetDateTime");
				case TypeCode.Char:
					return drType.GetMethod("GetChar");
				case TypeCode.String:
					return drType.GetMethod("GetString");
				case TypeCode.Single:
					return drType.GetMethod("GetFloat");
				case TypeCode.Double:
					return drType.GetMethod("GetDouble");
				case TypeCode.Decimal:
					return drType.GetMethod("GetDecimal");
				default:
					if (type == typeof(Guid))
					{
						return drType.GetMethod("GetGuid");
					}
					// TODO: byte[]? char[]?
					break;
			}

			throw new NotSupportedException();
		}



		public CompiledDataBinder(ReadOnlyCollection<DbColumn> schema)
		{
			// A couple notable optimizations:
			// All access is done via strongly-typed Get[Type] (GetInt32, GetString, etc) methods
			// This avoids boxing/unboxing values, which would happen with GetValue or indexer property.
			// If the schema claims a column is not nullable, then we avoid calling IsDBNull, 
			// which can be costly in some scenarios (and is never free).

			var ordinalMap =
				schema
				.Where(c => !string.IsNullOrEmpty(c.ColumnName))
				.Select((c, i) => new { Column = c, Idx = c.ColumnOrdinal ?? throw new ArgumentException() })
				.ToDictionary(p => p.Column.ColumnName, p => (p.Column, p.Idx));

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

			var drType = typeof(IDataRecord);

			var targetType = typeof(T);

			var recordParam = Expression.Parameter(drType);
			var itemParam = Expression.Parameter(targetType);
			var localsMap = new Dictionary<Type, ParameterExpression>();
			var locals = new List<ParameterExpression>();

			var bodyExpressions = new List<Expression>();

			var isDbNullMethod = drType.GetMethod("IsDBNull");

			foreach (var property in targetType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
			{
				var columnOrdinal = property.GetCustomAttribute<ColumnOrdinalAttribute>()?.Ordinal;
				var columnName = property.GetCustomAttribute<ColumnNameAttribute>()?.Name ?? property.Name;

				var col = GetCol(columnOrdinal, columnName);
				if (col == null)
				{
					// TODO: potentially add an argument to throw if there is an unbound property?
					continue;
				}

				var ordinal = col.ColumnOrdinal ?? columnOrdinal;

				if (ordinal == null)
				{
					// this means the column didn't know it's own ordinal, and neither did the property.
					continue;
				}

				var setter = property.GetSetMethod(true);
				var paramType = setter.GetParameters()[0].ParameterType;

				var accessorMethod = GetAccessorMethod(col.DataType);

				var ordinalExpr = Expression.Constant(ordinal, typeof(int));
				var accessorExpr = Expression.Call(recordParam, accessorMethod, ordinalExpr);

				Expression expr = GetCoerceExpression(accessorExpr, paramType);

				if (col.AllowDBNull != false)
				{
					// roughly
					// item.Property = record.IsDBNull(idx) ? default : Coerce(record.Get[Type](idx));
					expr =
						Expression.Condition(
							Expression.Call(recordParam, isDbNullMethod, ordinalExpr),
							Expression.Default(paramType),
							Expression.New(
								paramType.GetConstructor(
									new Type[] { paramType.GetGenericArguments()[0] }
								),
								accessorExpr
							)
						);

				}

				var setExpr = Expression.Call(itemParam, setter, expr);
				bodyExpressions.Add(setExpr);
			}

			var body = Expression.Block(locals, bodyExpressions);

			var lf = Expression.Lambda<Action<IDataRecord, T>>(body, recordParam, itemParam);
			this.f = lf.Compile();
		}

		static Expression GetCoerceExpression(Expression ex, Type targetType)
		{
			var sourceType = ex.Type;
			// Direct binding, this is fairly common.
			// The code below would end up doing this anyway.
			if (sourceType == targetType)
			{
				return ex;
			}

			Expression? expr = null;

			// If the target is nullable coerce to the underlying type.
			var underlyingType = Nullable.GetUnderlyingType(targetType);
			Type? nullableType = null;
			bool isNullable = underlyingType != null;
			if (underlyingType != null)
			{
				nullableType = targetType;
				targetType = underlyingType;
			}

			if (sourceType == targetType)
			{
				expr = ex;
			}
			else
			{
				if (targetType.IsEnum)
				{
					var enumBaseType = Enum.GetUnderlyingType(targetType);
					if (sourceType == enumBaseType)
					{
						expr = Expression.Convert(expr, targetType);
					}
					else
					{


					}
				}
				else
				{

				}
			}

			if (isNullable && expr != null)
			{
				expr =
					Expression.New(
						nullableType!.GetConstructor(
							new Type[] { underlyingType! }
						),
						ex
					);
			}

			return expr ?? throw new NotSupportedException();
		}

		void IDataBinder<T>.Bind(IDataRecord record, T item)
		{
			f(record, item);
		}
	}
}
