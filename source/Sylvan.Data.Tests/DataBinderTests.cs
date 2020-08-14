using Sylvan.Benchmarks;
using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Xunit;

namespace Sylvan.Data
{

	class MyDataRecord
	{
		[ColumnOrdinal(0)]
		public int Id { get; private set; }
		public string Name { get; private set; }
		public DateTime Date { get; private set; }
	}

	public class DataBinderTests
	{
		static readonly Lazy<ReadOnlyCollection<DbColumn>> Schema = new Lazy<ReadOnlyCollection<DbColumn>>(() => BuildSchema());

		static ReadOnlyCollection<DbColumn> BuildSchema()
		{
			var sb = new Schema.Builder();
			sb.AddColumn("Id", DbType.Int32, false);
			sb.AddColumn("Name", DbType.String, false);
			sb.AddColumn("Date", DbType.DateTime, true);
			var schema = sb.Build();
			return schema.GetColumnSchema();
		}

		static void Bind()
		{

		}

		[Fact]
		public void BenchReflection()
		{
			new DataBinderBenchmarks().Reflection();
		}

		[Fact]
		public void BenchCompiled()
		{
			new DataBinderBenchmarks().Compiled();
		}

		[Fact]
		public void Test1()
		{
			var schema = BuildSchema();
			var binder = new ReflectionDataBinder<MyDataRecord>(schema);

			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			while (data.Read())
			{
				var item = binder.Bind(data);
			}

		}

		[Fact]
		public void BinderExperiment()
		{
			var schema = BuildSchema();

			var ordinalMap =
				schema
				.Where(c => !string.IsNullOrEmpty(c.ColumnName))
				.Select((c, i) => new { Column = c, Idx = i })
				.ToDictionary(p => p.Column.ColumnName, p => (p.Column, p.Idx));

			var drType = typeof(IDataRecord);

			var targetType = typeof(MyDataRecord);

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

			var type = typeof(MyDataRecord);

			var args = new object[1];
			foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
			{
				args[0] = null;
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
					default:
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


			var lf =  Expression.Lambda<Func<IDataRecord, MyDataRecord>>(body, param);
			var sw = Stopwatch.StartNew();
			var f = lf.Compile();
			sw.Stop();

			sw = Stopwatch.StartNew();
			f = lf.Compile();
			sw.Stop();

			var binder = new ReflectionDataBinder<MyDataRecord>(schema);

			var csvData = "Id,Name,Date\n1,Test,2020-08-12\n";
			var tr = new StringReader(csvData);
			var opts = new CsvDataReaderOptions() { Schema = new CsvSchema(schema) };
			DbDataReader data = CsvDataReader.Create(tr, opts);

			while (data.Read())
			{
				var x = f(data);
			}
		}
	}
}
