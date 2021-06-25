//using System;
//using System.Collections.Generic;
//using System.Data;
//using System.Data.Common;
//using System.Globalization;
//using System.Linq;
//using System.Linq.Expressions;
//using System.Reflection;
//using System.Runtime.Serialization;

//namespace Sylvan.Data
//{
//	sealed class DynamicDataBinderFactory<T>
//	{
//		class DynamicDataBinder : IDataBinder<T>
//		{
//			int[] ordinalMap;
//			object[] accessors;

//			Action<DynamicDataBinder, IDataRecord, T> bindAction;

//			public ExampleDataBinder(IReadOnlyList<DbColumn> readerSchema)
//			{

//			}

//			public void Bind(IDataRecord record, T item)
//			{
//				bindAction(this, record, item);
//			}
//		}

//		internal DynamicDataBinderFactory(DataBinderOptions opts)
//		{
			
//			var recordType = typeof(T);

//			var recordParam = Expression.Parameter(DataBinder.IDataRecordType);
//			var contextParam = Expression.Parameter(typeof(BinderContext));
//			var itemParam = Expression.Parameter(recordType);
//			var debugParam = Expression.Parameter(typeof(string));
//			//var localsMap = new Dictionary<Type, ParameterExpression>();
//			var locals = new List<ParameterExpression>();
//			var bodyExpressions = new List<Expression>();

//			var cultureInfoExpr = Expression.Variable(typeof(CultureInfo));
//			locals.Add(cultureInfoExpr);
//			var idxExpr = Expression.Variable(typeof(int));
//			locals.Add(idxExpr);
//			locals.Add(debugParam);

//			// To provide special handling empty string as a null for a nullable primtivite type
//			// we want to construct the following:
//			// tempStr = GetString(ordinal);
//			// if(!IsNullString(tempStr)) { target.Value = double.Parse(tempStr); }
//			// specifically, we want to avoid calling GetString(ordinal) twice.
//			// so we use this singluar temporary variable for that purpose.
//			var tempStrExpr = Expression.Variable(typeof(string));
//			locals.Add(tempStrExpr);

//			bodyExpressions.Add(
//				Expression
//				.Assign(
//					cultureInfoExpr,
//					Expression.Field(
//						contextParam,
//						typeof(DynamicDataBinder).GetField("cultureInfo")
//					)
//				)
//			);

//			var stateVar = Expression.Variable(typeof(object[]));
//			locals.Add(stateVar);
//			bodyExpressions.Add(
//				Expression
//				.Assign(
//					stateVar,
//					Expression.Field(
//						contextParam,
//						typeof(DynamicDataBinder).GetField("state")
//					)
//				)
//			);

//			var properties =
//				recordType
//				.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
//				.Where(p => p.GetSetMethod() != null)
//				.ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);

//			foreach (var kvp in properties.ToArray())
//			{
//				var key = kvp.Key;
//				var property = kvp.Value;

//				var memberAttribute = property.GetCustomAttribute<DataMemberAttribute>();
//				var columnOrdinal = memberAttribute?.Order;
//				var columnName = memberAttribute?.Name ?? property.Name;

//				var setter = property.GetSetMethod(true);
//				if (setter == null)
//				{
//					//TODO: can/should we handle properties that have auto-backed fields?
//					continue;
//				}

//				var parameters = setter.GetParameters();

//				if (parameters.Length > 1)
//				{
//					// when would this happen? multidimensional indexers?
//					throw new NotSupportedException();
//				}

//				var targetType = parameters[0].ParameterType!;
//			}
//		}

//	}
//}
