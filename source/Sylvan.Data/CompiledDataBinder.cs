using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Sylvan.Data
{
	sealed class CompiledDataBinderFactory<T> : BinderFactory<T>
	{
		ReadOnlyCollection<DbColumn> metaSchema;

		public CompiledDataBinderFactory(ReadOnlyCollection<DbColumn> metaSchema)
		{
			this.metaSchema = metaSchema;
		}

		public override IDataBinder<T> CreateBinder(ReadOnlyCollection<DbColumn> schema)
		{
			return new CompiledDataBinder<T>(metaSchema, schema);
		}
	}

	sealed class CompiledDataBinder<T> : IDataBinder<T>
	{
		Action<IDataRecord, object[], T> f;

		object[] state = Array.Empty<object>();

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

		internal CompiledDataBinder(ReadOnlyCollection<DbColumn> physicalSchema) : this(physicalSchema, physicalSchema)
		{
		}

		internal CompiledDataBinder(ReadOnlyCollection<DbColumn> logicalSchema, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			// A couple notable optimizations:
			// All access is done via strongly-typed Get[Type] (GetInt32, GetString, etc) methods
			// This avoids boxing/unboxing values, which would happen with GetValue/indexer property.
			// If the schema claims a column is not nullable, then avoid calling IsDBNull, 
			// which can be costly in some scenarios (and is never free).
			var state = new List<object>();
			var stateIdx = 0;

			var ordinalMap =
				logicalSchema
				.Where(c => !string.IsNullOrEmpty(c.ColumnName))
				.ToDictionary(p => p.ColumnName, p => p);

			var seriesMap =
				logicalSchema
				.OfType<Schema.SchemaColumn>()
				.Where(c => c.IsSeries == true)
				.ToDictionary(p => p.SeriesName, p => p);

			DbColumn? GetCol(int? idx, string? name)
			{
				if (!string.IsNullOrEmpty(name))
				{
					// interesting that this needs to be annoted with not-null
					if (ordinalMap.TryGetValue(name!, out var c))
					{
						return c;
					}
					if(seriesMap.TryGetValue(name, out var sc))
					{
						return sc;
					}
				}
				if (idx != null)
				{
					return physicalSchema[idx.Value];
				}
				return null;
			}

			var drType = typeof(IDataRecord);

			var targetType = typeof(T);

			var recordParam = Expression.Parameter(drType);
			var stateParam = Expression.Parameter(typeof(object[]));
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

				// TODO: potentially use the properties dynamically instead of depending on the type here? 
				// Not of much value probably.
				var schemaCol = col as Schema.SchemaColumn;
				var isSeries = schemaCol?.IsSeries == true;

				if (isSeries)
				{

					// var acc = (DataSeriesAccessor<DateTime,int>)state[1];
					// target.Series = new DateSeries<DateTime>(acc.Minimum, acc.GetValues(reader));
					var sct = Type.GetTypeCode(schemaCol!.SeriesType);
					object seriesAccessor = GetDataSeriesAccessor(schemaCol, physicalSchema);
					stateIdx = state.Count;
					state.Add(seriesAccessor);

					var setter = property.GetSetMethod(true);
					var paramType = setter.GetParameters()[0].ParameterType;

					var seriesAccType = seriesAccessor.GetType();
					var accLocal = Expression.Variable(seriesAccType);


					var stateIdxExpr = Expression.Constant(stateIdx, typeof(int));

					var setLocalExpr =
						Expression.Assign(
							accLocal,
							Expression.Convert(
								Expression.ArrayAccess(
									stateParam,
									stateIdxExpr
								),
								seriesAccType
							)
						);
					// TODO: need to handle other series types.
					var seriesType = typeof(DateSeries<>).MakeGenericType(schemaCol.DataType);
					var seriesCtor = seriesType.GetConstructor(new Type[] { typeof(DateTime), typeof(IEnumerable<>).MakeGenericType(schemaCol.DataType) });

					// TODO: this is terrible. Just push this down as a constructor on Series.
					var ctorExpr =
						Expression.New(
							seriesCtor,
							Expression.Property(
								accLocal,
								seriesAccType.GetProperty("Minimum")
							),
							Expression.Call(
								accLocal,
								seriesAccType.GetMethod("GetValues"),
								recordParam
							)
						);
					
					var setExpr = Expression.Call(itemParam, setter, ctorExpr);

					var block = Expression.Block(new ParameterExpression[] { accLocal }, new Expression[] { setLocalExpr, setExpr });

					bodyExpressions.Add(block);

				}
				else
				{

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
					Expression accessorExpr = Expression.Call(recordParam, accessorMethod, ordinalExpr);

					Expression expr = GetConvertExpression(accessorExpr, paramType);

					if (col.AllowDBNull != false)
					{
						// roughly:
						// item.Property = record.IsDBNull(idx) ? default : Coerce(record.Get[Type](idx));
						expr =
							Expression.Condition(
								Expression.Call(recordParam, isDbNullMethod, ordinalExpr),
								Expression.Default(paramType),
								expr
							);
					}

					var setExpr = Expression.Call(itemParam, setter, expr);
					bodyExpressions.Add(setExpr);
				}
			}

			this.state = state.ToArray();
			var body = Expression.Block(locals, bodyExpressions);

			var lf = Expression.Lambda<Action<IDataRecord, object[], T>>(body, recordParam, stateParam, itemParam);
			this.f = lf.Compile();
		}

		static object GetDataSeriesAccessor(Schema.SchemaColumn seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			switch (Type.GetTypeCode(seriesCol.SeriesType))
			{
				case TypeCode.Int32:
					throw new NotImplementedException();
				case TypeCode.DateTime:
					return GetDateSeriesAccessor(seriesCol, physicalSchema);
				default:
					throw new NotSupportedException();
			}
		}

		static object GetDateSeriesAccessor(Schema.SchemaColumn seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			var fmt = seriesCol.SeriesHeaderFormat ?? "{Date}"; //asdf{Date}qwer => ^asdf(.*)qwer$

			var accessorType = typeof(DataSeriesAccessor<,>).MakeGenericType(typeof(DateTime), seriesCol.DataType);
			var cols = new List<DataSeriesColumn<DateTime>>();
			var i = fmt.IndexOf("{Date}", StringComparison.OrdinalIgnoreCase);

			var prefix = fmt.Substring(0, i);
			var suffix = fmt.Substring(i + "{Date}".Length);
			var pattern = "^" + Regex.Escape(prefix) + "(.+)" + Regex.Escape(suffix) + "$";

			var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
			foreach(var col in physicalSchema)
			{
				var name = col.ColumnName;
				var match = regex.Match(name);
				if (match.Success)
				{
					var dateStr = match.Captures[0].Value;
					DateTime d;
					if(DateTime.TryParse(dateStr, out d))
					{
						var ordinal = col.ColumnOrdinal!.Value;
						cols.Add(new DataSeriesColumn<DateTime>(name, d, ordinal));
					}
				}
			}

			return Activator.CreateInstance(accessorType, cols);
		}

		static Expression GetConvertExpression(Expression getterExpr, Type targetType)
		{
			var sourceType = getterExpr.Type;
			// Direct binding, this is fairly common.
			// The code below would end up doing this anyway.
			if (sourceType == targetType)
			{
				return getterExpr;
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
				expr = getterExpr;
			}
			else
			{
				if (targetType.IsEnum)
				{
					var enumBaseType = Enum.GetUnderlyingType(targetType);
					if (sourceType == enumBaseType)
					{
						expr = Expression.Convert(getterExpr, targetType);
					}
					else
					{
						if (sourceType == typeof(string))
						{
							expr =
								Expression.Call(
									EnumParseMethod.MakeGenericMethod(targetType),
									getterExpr
								);
						}
						else
						{
							throw new NotSupportedException();
							// not sure what else would be supportable here.
						}
					}
				}
				else
				{
					if (sourceType.IsValueType && targetType.IsValueType)
					{
						// handle primitive conversions with casts.
						expr = Expression.Convert(getterExpr, targetType);
					}
					else
					{
						if (targetType.IsClass && targetType != typeof(string))
						{
							var ctor = targetType.GetConstructor(new Type[] { getterExpr.Type });
							if (ctor == null)
							{
								throw new NotSupportedException();
							}
							expr = Expression.New(ctor, getterExpr);
						}
						else
						{
							// handle everything else with Convert.ChangeType?
							expr = Expression.Call(
								ChangeTypeMethod,
								getterExpr,
								Expression.Constant(targetType)
							);

							if (targetType.IsValueType)
							{
								expr = Expression.Unbox(expr, targetType);
							}
							else
							{
								expr = Expression.Convert(expr, targetType);
							}
						}
					}
				}
			}

			if (isNullable && expr != null)
			{
				expr =
					Expression.New(
						nullableType!.GetConstructor(
							new Type[] { underlyingType! }
						),
						getterExpr
					);
			}

			return expr ?? throw new NotSupportedException();
		}

		static MethodInfo EnumParseMethod =
			typeof(BinderMethods)
			.GetMethod("ParseEnum", BindingFlags.NonPublic | BindingFlags.Static);

		static MethodInfo ChangeTypeMethod =
			typeof(BinderMethods)
			.GetMethod("ChangeType", BindingFlags.NonPublic | BindingFlags.Static);

		void IDataBinder<T>.Bind(IDataRecord record, T item)
		{
			f(record, this.state, item);
		}
	}

	static class BinderMethods
	{
		static object ChangeType(object value, Type type)
		{
			return Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
		}

		static TE ParseEnum<TE>(string value) where TE : struct, Enum
		{
			if (Enum.TryParse(value, true, out TE e))
			{
				return e;
			}
			else
			{
				throw new InvalidEnumValueDataBinderException(typeof(TE), value);
			}
		}
	}

	public class DataBinderException : Exception
	{
	}

	public class InvalidEnumValueDataBinderException : DataBinderException
	{
		public string Value { get; }
		public Type EnumType { get; }

		public InvalidEnumValueDataBinderException(Type enumType, string value)
		{
			this.EnumType = enumType;
			this.Value = value;
		}
	}
}
