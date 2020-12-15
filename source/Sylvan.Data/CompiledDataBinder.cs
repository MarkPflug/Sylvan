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
					return drType.GetMethod("GetBoolean")!;
				case TypeCode.Byte:
					return drType.GetMethod("GetByte")!;
				case TypeCode.Int16:
					return drType.GetMethod("GetInt16")!;
				case TypeCode.Int32:
					return drType.GetMethod("GetInt32")!;
				case TypeCode.Int64:
					return drType.GetMethod("GetInt64")!;
				case TypeCode.DateTime:
					return drType.GetMethod("GetDateTime")!;
				case TypeCode.Char:
					return drType.GetMethod("GetChar")!;
				case TypeCode.String:
					return drType.GetMethod("GetString")!;
				case TypeCode.Single:
					return drType.GetMethod("GetFloat")!;
				case TypeCode.Double:
					return drType.GetMethod("GetDouble")!;
				case TypeCode.Decimal:
					return drType.GetMethod("GetDecimal")!;
				default:
					if (type == typeof(Guid))
					{
						return drType.GetMethod("GetGuid")!;
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
				.ToDictionary(p => string.IsNullOrEmpty(p.SeriesName) ? "Values" : p.SeriesName, p => p);

			DbColumn? GetCol(int? idx, string? name)
			{
				if (!string.IsNullOrEmpty(name))
				{
					// interesting that this needs to be annoted with not-null
					if (ordinalMap.TryGetValue(name!, out var c))
					{
						return c;
					}
					if (seriesMap.TryGetValue(name, out var sc))
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

			var isDbNullMethod = drType.GetMethod("IsDBNull")!;

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
					// TODO: I don't like that I'm special-casing the Sylvan.Data.Series type here.
					// Can this be done in a more generic way that would support BYO type?

					// var acc = (DataSeriesAccessor<DateTime,int>)state[1];
					// target.Series = new DateSeries<DateTime>(acc.Minimum, acc.GetValues(reader));
					var sct = Type.GetTypeCode(schemaCol!.SeriesType);
					object seriesAccessor = GetDataSeriesAccessor(schemaCol, physicalSchema);
					stateIdx = state.Count;
					state.Add(seriesAccessor);

					var setter = property.GetSetMethod(true);
					if(setter == null)
					{
						// TODO:
						continue;
					}
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

					Type seriesType =
						schemaCol.SeriesType == typeof(DateTime)
						? typeof(DateSeries<>)
						: typeof(Series<>);
					seriesType = seriesType.MakeGenericType(schemaCol.DataType!);

					var seriesCtor = seriesType.GetConstructor(new Type[] { schemaCol.SeriesType!, typeof(IEnumerable<>).MakeGenericType(schemaCol.DataType!) })!;

					// TODO: push this down as a constructor on Series?
					var ctorExpr =
						Expression.New(
							seriesCtor,
							Expression.Property(
								accLocal,
								seriesAccType.GetProperty("Minimum")!
							),
							Expression.Call(
								accLocal,
								seriesAccType.GetMethod("GetValues")!,
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
					if (setter == null)
					{
						//TODO: can we handle auto backed properties? what about the new init-only?
						continue;
					}

					var paramType = setter.GetParameters()[0].ParameterType;

					var accessorMethod = GetAccessorMethod(col.DataType!);

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

		enum ConversionType
		{
			NotSupported = 0,
			Identity = 1,
			Cast = 2,
			ToString = 3,
			UnsafeCast = 4,
			Parse = 5,
		}

		// TODO: validate this table.
		static byte[][] Conversion = new byte[][] {
					// Empty, Object, DBNull, Boolean, Char, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Single, Double, Decimal, DateTime, BLANK, String
			//Empty = 0,
					new byte[20],
			//Object = 1,
					new byte[20],
			//DBNull = 2,
					new byte[20],
			//Boolean = 3,
			new byte[20] { 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//Char = 4,
			new byte[20] { 0, 0, 0, 0, 1, 4, 4, 4, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//SByte = 5,
			new byte[20] { 0, 0, 0, 0, 2, 1, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//Byte = 6,
			new byte[20] { 0, 0, 0, 0, 4, 4, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//Int16 = 7,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 1, 4, 2, 2, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//UInt16 = 8,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 1, 2, 2, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//Int32 = 9,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 4, 1, 4, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//UInt32 = 10,			
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 1, 2, 2, 2, 2, 2, 0, 0, 3, 0 },
			//Int64 = 11,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 1, 4, 2, 2, 2, 0, 0, 3, 0 },
			//UInt64 = 12,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4, 1, 2, 2, 2, 0, 0, 3, 0 },
			//Single = 13,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 1, 2, 4, 0, 0, 3, 0 },
			//Double = 14,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 1, 4, 0, 0, 3, 0 },
			//Decimal = 15,
			new byte[20] { 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 1, 0, 0, 3, 0 },
			//DateTime = 16,
			new byte[20] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0 },
			// BLANK
			new byte[20],
			//String = 18
			new byte[20] { 0, 0, 0, 5, 0, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 0, 1, 0 },
		};

		static ConversionType GetConversionType(TypeCode src, TypeCode dst)
		{
			return (ConversionType)Conversion[(int)src][(int)dst];
		}

		static Expression? Convert(Expression expr, Type dstType, Expression cultureInfoExpr)
		{
			var srcType = expr.Type;
			var srcTypeCode = Type.GetTypeCode(srcType);
			var dstTypeCode = Type.GetTypeCode(dstType);

			var conversionType = GetConversionType(srcTypeCode, dstTypeCode);

			
			switch(conversionType)
			{
				case ConversionType.NotSupported:
					return null;
				case ConversionType.Identity:
					return expr;
				case ConversionType.Cast:
					return Expression.MakeUnary(ExpressionType.Convert, expr, dstType);
				case ConversionType.UnsafeCast:
					// TODO: this should be opt-in when building the binder.
					return Expression.MakeUnary(ExpressionType.ConvertChecked, expr, dstType);
				case ConversionType.ToString:
					var toStringMethod = srcType.GetMethod("ToString", BindingFlags.Instance | BindingFlags.Public, null, new Type[] { typeof(IFormatProvider) }, null);
					if (toStringMethod != null)
					{
						return Expression.Call(expr, toStringMethod, cultureInfoExpr);
					}
					return Expression.Call(expr, typeof(object).GetMethod("ToString")!);
				case ConversionType.Parse:
					var parseMethod = dstType.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
					if (parseMethod != null)
					{
						return Expression.Call(parseMethod, new[] { expr, cultureInfoExpr });
					}

					parseMethod = dstType.GetMethod("Parse", new[] { typeof(string) });
					if (parseMethod != null)
					{
						return Expression.Call(parseMethod, new[] { expr });
					}
					return null;
			}



			return null;
			//var dstCtor = dstType.GetConstructor(new Type[] { typeof(string), typeof(IFormatProvider) });
			//if (dstCtor != null)
			//{
			//	return Expression.New(dstCtor, expr, cultureInfoExpr);
			//}
			//dstType.GetConstructor(new Type[] { typeof(string) });
			//if (dstCtor != null)
			//{
			//	return Expression.New(dstCtor, expr);
			//}
		}

		static object GetDataSeriesAccessor(Schema.SchemaColumn seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			switch (Type.GetTypeCode(seriesCol.SeriesType))
			{
				case TypeCode.Int32:
					return GetIntSeriesAccessor(seriesCol, physicalSchema);
				case TypeCode.DateTime:
					return GetDateSeriesAccessor(seriesCol, physicalSchema);
				default:
					throw new NotSupportedException();
			}
		}

		static object GetDateSeriesAccessor(Schema.SchemaColumn seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			var fmt = seriesCol.SeriesHeaderFormat ?? Schema.DateSeriesMarker; //asdf{Date}qwer => ^asdf(.+)qwer$

			var accessorType = typeof(DataSeriesAccessor<,>).MakeGenericType(typeof(DateTime), seriesCol.DataType!);
			var cols = new List<DataSeriesColumn<DateTime>>();
			var i = fmt.IndexOf(Schema.DateSeriesMarker, StringComparison.OrdinalIgnoreCase);

			var prefix = fmt.Substring(0, i);
			var suffix = fmt.Substring(i + Schema.DateSeriesMarker.Length);
			var pattern = "^" + Regex.Escape(prefix) + "(.+)" + Regex.Escape(suffix) + "$";

			var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
			foreach (var col in physicalSchema)
			{
				var name = col.ColumnName;
				var match = regex.Match(name);
				if (match.Success)
				{
					var dateStr = match.Captures[0].Value;
					DateTime d;
					if (DateTime.TryParse(dateStr, out d))
					{
						var ordinal = col.ColumnOrdinal!.Value;
						cols.Add(new DataSeriesColumn<DateTime>(name, d, ordinal));
					}
				}
			}

			return Activator.CreateInstance(accessorType, cols)!;
		}

		static object GetIntSeriesAccessor(Schema.SchemaColumn seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			var fmt = seriesCol.SeriesHeaderFormat ?? Schema.IntegerSeriesMarker; //col{Integer} => ^col(\d+)$

			var accessorType = typeof(DataSeriesAccessor<,>).MakeGenericType(typeof(int), seriesCol.DataType!);
			var cols = new List<DataSeriesColumn<int>>();
			var i = fmt.IndexOf(Schema.IntegerSeriesMarker, StringComparison.OrdinalIgnoreCase);

			var prefix = fmt.Substring(0, i);
			var suffix = fmt.Substring(i + Schema.IntegerSeriesMarker.Length);
			var pattern = "^" + Regex.Escape(prefix) + "(\\d+)" + Regex.Escape(suffix) + "$";

			var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
			foreach (var col in physicalSchema)
			{
				var name = col.ColumnName;
				var match = regex.Match(name);
				if (match.Success)
				{
					var intStr = match.Captures[0].Value;
					int d;
					if (int.TryParse(intStr, out d))
					{
						var ordinal = col.ColumnOrdinal!.Value;
						cols.Add(new DataSeriesColumn<int>(name, d, ordinal));
					}
				}
			}

			return Activator.CreateInstance(accessorType, cols)!;
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

				var nullableCtor = nullableType!.GetConstructor(new Type[] { underlyingType! })!;

				if (getterExpr.Type == typeof(string))
				{
					var tempVar = Expression.Variable(getterExpr.Type);

					// var tempVar = getter();
					// retur IsNullString(tempVar) ? new double?() : 

					expr =
						Expression.Block(
							new ParameterExpression[] {
								tempVar
							},
							new Expression[]
							{
								Expression.Assign(tempVar, getterExpr),
								Expression.Condition(
									Expression.Call(
										IsNullStringMethod,
										tempVar
									),
									Expression.Default(nullableType),
									Expression.New(
										nullableCtor,
										Expression.Call(
											ChangeTypeMethod,
											tempVar,
											Expression.Constant(underlyingType)
										)
									)
								)
							}
						);
				}
				else
				{
					expr =
						Expression.New(
							nullableCtor,
							getterExpr
						);
				}
			}

			return expr ?? throw new NotSupportedException();
		}

		static MethodInfo EnumParseMethod =
			typeof(BinderMethods)
			.GetMethod("ParseEnum", BindingFlags.NonPublic | BindingFlags.Static)!;

		static MethodInfo ChangeTypeMethod =
			typeof(BinderMethods)
			.GetMethod("ChangeType", BindingFlags.NonPublic | BindingFlags.Static)!;

		static MethodInfo IsNullStringMethod =
			typeof(BinderMethods)
			.GetMethod("IsNullString", BindingFlags.NonPublic | BindingFlags.Static)!;

		void IDataBinder<T>.Bind(IDataRecord record, T item)
		{
			f(record, this.state, item);
		}
	}

	static class BinderMethods
	{
		

		static bool IsNullString(string str)
		{
			return string.IsNullOrWhiteSpace(str);
		}

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

	public sealed class InvalidEnumValueDataBinderException : DataBinderException
	{
		public string Value { get; }
		public Type EnumType { get; }

		internal InvalidEnumValueDataBinderException(Type enumType, string value)
		{
			this.EnumType = enumType;
			this.Value = value;
		}
	}
}
