using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;

namespace Sylvan.Data
{
	sealed class CompiledDataBinder<T> : IDataBinder<T>
	{
		Action<IDataRecord, BinderContext, T> recordBinderFunction;

		object[] state = Array.Empty<object>();
		CultureInfo cultureInfo;

		static readonly Type drType = typeof(IDataRecord);

		internal CompiledDataBinder(
			DataBinderOptions opts,
			ReadOnlyCollection<DbColumn> physicalSchema
		) : this(opts, physicalSchema, physicalSchema)
		{
		}

		internal CompiledDataBinder(
			DataBinderOptions opts,
			ReadOnlyCollection<DbColumn> physicalSchema,
			ReadOnlyCollection<DbColumn> logicalSchema
		)
		{
			// A couple notable optimizations:
			// All access is done via strongly-typed Get[Type] (GetInt32, GetString, etc) methods
			// This avoids boxing/unboxing values, which would happen with GetValue/indexer property.
			// If the schema claims a column is not nullable, then avoid calling IsDBNull, 
			// which can be costly in some scenarios (and is never free).

			// stores contextual state used by the binder.
			var state = new List<object>();

			this.cultureInfo = opts.Culture ?? CultureInfo.InvariantCulture;

			var colNamer = opts.ColumnNamer;

			var namedCols = logicalSchema.Where(c => !string.IsNullOrEmpty(c.ColumnName)).ToList();
			var nameMap =
				namedCols
				.ToDictionary(p => p.ColumnName, p => p, StringComparer.OrdinalIgnoreCase);

			if (colNamer != null)
			{
				foreach (var col in namedCols)
				{
					var ordinal = col.ColumnOrdinal;
					var name = col.ColumnName;
					var cleanName = colNamer(name, ordinal ?? -1); //TODO
					if (cleanName != null && !nameMap.ContainsKey(cleanName))
					{
						nameMap.Add(cleanName, col);
					}
				}
			}

			logicalSchema
				.Where(c => !string.IsNullOrEmpty(c.ColumnName));

			var seriesMap =
				logicalSchema
				.OfType<Schema.Column>()
				.Where(c => c.IsSeries == true)
				.ToDictionary(p => string.IsNullOrEmpty(p.SeriesName) ? "Values" : p.SeriesName, p => p, StringComparer.OrdinalIgnoreCase);

			DbColumn? GetCol(int? idx, string? name)
			{
				if (!string.IsNullOrEmpty(name))
				{
					// interesting that this needs to be annoted with not-null
					if (nameMap.TryGetValue(name!, out var c))
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
			var isDbNullMethod = drType.GetMethod("IsDBNull")!;

			var recordType = typeof(T);

			var recordParam = Expression.Parameter(drType);
			var contextParam = Expression.Parameter(typeof(BinderContext));
			var itemParam = Expression.Parameter(recordType);
			var localsMap = new Dictionary<Type, ParameterExpression>();
			var locals = new List<ParameterExpression>();
			var bodyExpressions = new List<Expression>();

			var cultureInfoExpr = Expression.Variable(typeof(CultureInfo));
			locals.Add(cultureInfoExpr);

			// To provide special handling empty string as a null for a nullable primtivite type
			// we want to construct the following:
			// tempStr = GetString(ordinal);
			// if(!IsNullString(tempStr)) { target.Value = double.Parse(tempStr); }
			// specifically, we want to avoid calling GetString(ordinal) twice.
			// so we use this singluar temporary variable for that purpose.
			var tempStrExpr = Expression.Variable(typeof(string));
			locals.Add(tempStrExpr);

			bodyExpressions.Add(
				Expression
				.Assign(
					cultureInfoExpr,
					Expression.Field(
						contextParam,
						typeof(BinderContext).GetField("cultureInfo")
					)
				)
			);

			var stateVar = Expression.Variable(typeof(object[]));
			locals.Add(stateVar);
			bodyExpressions.Add(
				Expression
				.Assign(
					stateVar,
					Expression.Field(
						contextParam,
						typeof(BinderContext).GetField("state")
					)
				)
			);

			foreach (var property in recordType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
			{
				var memberAttribute = property.GetCustomAttribute<DataMemberAttribute>();
				var columnOrdinal = memberAttribute?.Order;
				var columnName = memberAttribute?.Name ?? property.Name;

				var col = GetCol(columnOrdinal, columnName);
				Type colType = col?.DataType ?? typeof(object);
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
				if (setter == null)
				{
					//TODO: can/should we handle properties that have auto-backed fields?
					continue;
				}

				var targetType = setter.GetParameters()[0].ParameterType!;

				var accessorMethod = DataBinder.GetAccessorMethod(colType);

				var ordinalExpr = Expression.Constant(ordinal, typeof(int));

				Expression getterExpr = Expression.Call(recordParam, accessorMethod, ordinalExpr);

				Expression? expr = getterExpr;

				var sourceType = getterExpr.Type;

				// Direct binding, this is fairly common.
				// The code below would end up doing this anyway.
				if (sourceType == targetType)
				{
					expr = getterExpr;
				}
				else
				{
					// If the target is nullable coerce to the underlying type.
					var underlyingType = Nullable.GetUnderlyingType(targetType);
					Type? nullableType = null;
					bool isNullable = underlyingType != null;
					if (isNullable)
					{
						nullableType = targetType;
						targetType = underlyingType!;
					}

					if (targetType.IsEnum)
					{
						// for enums there are two supported scenarios
						// binding by name (string)
						// and binding by value (numeric).
						var enumBaseType = Enum.GetUnderlyingType(targetType);
						if (sourceType == enumBaseType)
						{
							expr = Expression.Convert(getterExpr, targetType);
							// TODO: validate that the value is valid for the enum?
						}
						else
						{
							if (sourceType == typeof(string))
							{
								// This method provides its own validation
								expr =
									Expression.Call(
										DataBinder.EnumParseMethod.MakeGenericMethod(targetType),
										getterExpr
									);
							}
							else
							{
								// not sure what else would be supportable here.
								throw new NotSupportedException();
							}
						}
					}
					else
					{
						expr = Convert(colType, getterExpr, targetType, cultureInfoExpr);

						if (expr == null)
						{
							if (col is Schema.Column c)
							{
								expr = BindSeries(property, c, physicalSchema, state, stateVar, recordParam, itemParam);
								bodyExpressions.Add(expr);
								continue;
							}
							else
							{
								var ctor = targetType.GetConstructor(new Type[] { colType });
								if (ctor != null)
								{
									expr = Expression.New(
										ctor,
										Expression.MakeUnary(
											ExpressionType.Convert,
											getterExpr,
											colType
										)
									);
								}
								else
								{
									// TODO: should this only happen if the getterExpr is "object"?
									// what else could it be at this point?
									expr = Expression.Convert(getterExpr, targetType);
								}
							}
						}
					}

					Debug.Assert(expr != null);

					if (isNullable)
					{
						var nullableCtor = nullableType!.GetConstructor(new Type[] { underlyingType! })!;

						if (getterExpr.Type == typeof(string))
						{
							var tempVar = Expression.Variable(getterExpr.Type);

							var valueExpr = Convert(colType, tempVar, underlyingType!, cultureInfoExpr);

							// var tempVar = getter();
							// retur IsNullString(tempVar) ? default(double?) : double.parse(tempVar); 

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
												DataBinder.IsNullStringMethod,
												tempVar
											),
											Expression.Default(nullableType),
											Expression.New(
												nullableCtor,
												valueExpr
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

					if (expr == null)
					{
						throw new NotSupportedException();
					}
				}

				if (col.AllowDBNull != false)
				{
					// roughly:
					// item.Property = record.IsDBNull(idx) ? default : Coerce(record.Get[Type](idx));
					expr =
						Expression.Condition(
							Expression.Call(recordParam, isDbNullMethod, ordinalExpr),
							Expression.Default(expr.Type),
							expr
						);
				}

				var setExpr = Expression.Call(itemParam, setter, expr);
				bodyExpressions.Add(setExpr);
			}

			this.state = state.ToArray();
			var body = Expression.Block(locals, bodyExpressions);

			var lf = Expression.Lambda<Action<IDataRecord, BinderContext, T>>(body, recordParam, contextParam, itemParam);
			this.recordBinderFunction = lf.Compile();
		}

		static Expression BindSeries(
			PropertyInfo property,
			Schema.Column column,
			ReadOnlyCollection<DbColumn> physicalSchema,
			List<object> state,
			Expression stateParam,
			Expression recordParam,
			Expression itemParam
		)
		{
			Debug.Assert(column.IsSeries == true);

			// TODO: potentially use the properties dynamically instead of depending on the type here? 
			// Not of much value probably.

			// TODO: I don't like that I'm special-casing the Sylvan.Data.Series type here.
			// Can this be done in a more generic way that would support BYO type?
			//var sct = Type.GetTypeCode(column.SeriesType);

			object seriesAccessor = GetDataSeriesAccessor(column, physicalSchema);
			var stateIdx = state.Count;
			state.Add(seriesAccessor);

			var setter = property.GetSetMethod(true);
			if (setter == null)
			{
				// TODO:
				throw new NotSupportedException();
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
				column.SeriesType == typeof(DateTime)
				? typeof(DateSeries<>)
				: typeof(Series<>);

			seriesType = seriesType.MakeGenericType(column.DataType!);

			var seriesCtor = seriesType.GetConstructor(new Type[] { column.SeriesType!, typeof(IEnumerable<>).MakeGenericType(column.DataType!) })!;

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

			return block;
		}

		class BinderContext
		{
			public BinderContext(CultureInfo ci, object[] state)
			{
				this.cultureInfo = ci;
				this.state = state;
			}

			public CultureInfo cultureInfo;
			public object[] state;
		}

		enum ConversionType
		{
			// conversion is not supported or uses a constu
			NotSupported = 0,
			// no op
			Identity = 1,
			// simple primitive widening type cast
			Cast = 2,
			// convert via ToString operation
			ToString = 3,
			// simple primitive narrowing type cast, must be checked
			UnsafeCast = 4,
			// convert via Parse operation
			Parse = 5,
		}

		static int[] TypeCodeMap = new int[] {
			//Empty = 0,
			-1,
			//Object = 1,
			15,
			//DBNull = 2,
			-1,
			//Boolean = 3,
			0,
			//Char = 4,
			1,
			//SByte = 5,
			2,
			//Byte = 6,
			3,
			//Int16 = 7,
			4,
			//UInt16 = 8,
			5,
			//Int32 = 9,
			6,
			//UInt32 = 10,
			7,
			//Int64 = 11,
			8,
			//UInt64 = 12,
			9,
			//Single = 13,
			10,
			//Double = 14,
			11,
			//Decimal = 15,
			12,
			//DateTime = 16,
			13,
			// BLANK
			-1,
			//String = 18
			14,
		};

		static byte[] Conversion = new byte[16 * 16] {
			// Boolean, Char, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Single, Double, Decimal, DateTime, String, Object
			//Boolean = 3,
			1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 3, 0,
			//Char = 4,
			0, 1, 4, 4, 4, 2, 2, 2, 2, 2, 2, 2, 2, 0, 3, 0,
			//SByte = 5,
			0, 2, 1, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 3, 0,
			//Byte = 6,
			0, 4, 4, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 3, 0,
			//Int16 = 7,
			0, 4, 4, 4, 1, 4, 2, 2, 2, 2, 2, 2, 2, 0, 3, 0,
			//UInt16 = 8,
			0, 4, 4, 4, 4, 1, 2, 2, 2, 2, 2, 2, 2, 0, 3, 0,
			//Int32 = 9,
			0, 4, 4, 4, 4, 4, 1, 4, 2, 2, 2, 2, 2, 0, 3, 0,
			//UInt32 = 10,
			0, 4, 4, 4, 4, 4, 4, 1, 2, 2, 2, 2, 2, 0, 3, 0,
			//Int64 = 11,
			0, 4, 4, 4, 4, 4, 4, 4, 1, 4, 2, 2, 2, 0, 3, 0,
			//UInt64 = 12,
			0, 4, 4, 4, 4, 4, 4, 4, 4, 1, 2, 2, 2, 0, 3, 0,
			//Single = 13,
			0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 1, 2, 4, 0, 3, 0,
			//Double = 14,
			0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 1, 4, 0, 3, 0,
			//Decimal = 15,
			0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 1, 0, 3, 0,
			//DateTime = 16
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0,
			//String = 18
			5, 0, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 1, 5,
			//Object = 1
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0,
		};

		static ConversionType GetConversionType(TypeCode src, TypeCode dst)
		{
			int srcIdx = TypeCodeMap[(int)src];
			int dstIdx = TypeCodeMap[(int)dst];
			if (srcIdx == -1 || dstIdx == -1) return ConversionType.NotSupported;
			return (ConversionType)Conversion[srcIdx * 16 + dstIdx];
		}

		static Expression? Convert(Type colType, Expression expr, Type dstType, Expression cultureInfoExpr)
		{
			var srcType = expr.Type;
			var srcTypeCode = Type.GetTypeCode(srcType);
			var dstTypeCode = Type.GetTypeCode(dstType);

			var conversionType = GetConversionType(srcTypeCode, dstTypeCode);

			switch (conversionType)
			{
				case ConversionType.NotSupported:
					return null;
				case ConversionType.Identity:
					return expr;
				case ConversionType.Cast:
					return Expression.MakeUnary(ExpressionType.Convert, expr, dstType);
				case ConversionType.UnsafeCast:
					// TODO: should this be opt-in when constructing the binder?
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

		static object GetDataSeriesAccessor(Schema.Column seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			var method = typeof(DataBinder).GetMethod("GetSeriesAccessor", BindingFlags.Static | BindingFlags.NonPublic);
			method = method.MakeGenericMethod(new Type[] { seriesCol.DataType! });
			return method.Invoke(null, new object[] { seriesCol, physicalSchema });
		}

		//static object GetDateSeriesAccessor(Schema.Column seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		//{
		//	var fmt = seriesCol.SeriesHeaderFormat ?? Schema.DateSeriesMarker; //asdf{Date}qwer => ^asdf(.+)qwer$

		//	var accessorType = typeof(DataSeriesAccessor<,>).MakeGenericType(typeof(DateTime), seriesCol.DataType!);
		//	var cols = new List<DataSeriesColumn<DateTime>>();
		//	var i = fmt.IndexOf(Schema.DateSeriesMarker, StringComparison.OrdinalIgnoreCase);

		//	var prefix = fmt.Substring(0, i);
		//	var suffix = fmt.Substring(i + Schema.DateSeriesMarker.Length);
		//	var pattern = "^" + Regex.Escape(prefix) + "(.+)" + Regex.Escape(suffix) + "$";

		//	var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
		//	foreach (var col in physicalSchema)
		//	{
		//		var name = col.ColumnName;
		//		var match = regex.Match(name);
		//		if (match.Success)
		//		{
		//			var dateStr = match.Captures[0].Value;
		//			DateTime d;
		//			if (DateTime.TryParse(dateStr, out d))
		//			{
		//				var ordinal = col.ColumnOrdinal!.Value;
		//				cols.Add(new DataSeriesColumn<DateTime>(name, d, ordinal));
		//			}
		//		}
		//	}

		//	return Activator.CreateInstance(accessorType, cols)!;
		//}

		//static object GetIntSeriesAccessor(Schema.Column seriesCol, ReadOnlyCollection<DbColumn> physicalSchema)
		//{
		//	var fmt = seriesCol.SeriesHeaderFormat ?? Schema.IntegerSeriesMarker; //col{Integer} => ^col(\d+)$

		//	var accessorType = typeof(DataSeriesAccessor<,>).MakeGenericType(typeof(int), seriesCol.DataType!);
		//	var cols = new List<DataSeriesColumn<int>>();
		//	var i = fmt.IndexOf(Schema.IntegerSeriesMarker, StringComparison.OrdinalIgnoreCase);

		//	var prefix = fmt.Substring(0, i);
		//	var suffix = fmt.Substring(i + Schema.IntegerSeriesMarker.Length);
		//	var pattern = "^" + Regex.Escape(prefix) + "(\\d+)" + Regex.Escape(suffix) + "$";

		//	var regex = new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
		//	foreach (var col in physicalSchema)
		//	{
		//		var name = col.ColumnName;
		//		var match = regex.Match(name);
		//		if (match.Success)
		//		{
		//			var intStr = match.Captures[0].Value;
		//			int d;
		//			if (int.TryParse(intStr, out d))
		//			{
		//				var ordinal = col.ColumnOrdinal!.Value;
		//				cols.Add(new DataSeriesColumn<int>(name, d, ordinal));
		//			}
		//		}
		//	}

		//	return Activator.CreateInstance(accessorType, cols)!;
		//}

		void IDataBinder<T>.Bind(IDataRecord record, T item)
		{
			var context = new BinderContext(this.cultureInfo, this.state);

			recordBinderFunction(record, context, item);
		}
	}
}
