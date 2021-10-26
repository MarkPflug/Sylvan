using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	//public static class DataBinderFactory
	//{
	//	public static IDataBinder<T> Create<T>(this IDataBinderFactory<T> factory, DbDataReader reader, DataBinderOptions? options = null)
	//	{
	//		var schema = reader.GetColumnSchema();
	//		return factory.Create(schema, options);
	//	}

	//	public static IDataBinder<T> Create<T>(this IDataBinderFactory<T> factory, IReadOnlyList<DbColumn> schema)
	//	{
	//		return factory.Create(schema, null);
	//	}
	//}

	///// <summary>
	///// A factory for creating a data binder for a given schema.
	///// </summary>
	///// <typeparam name="T"></typeparam>
	//public interface IDataBinderFactory<T>
	//{
	//	IDataBinder<T> Create(IReadOnlyList<DbColumn> schema, DataBinderOptions? options);
	//}

	public interface IDataBinder
	{
		void Bind(DbDataReader record, object item);
	}

	public interface IDataBinder<T> : IDataBinder
	{
		void Bind(DbDataReader record, T item);
	}

	interface IDataSeriesBinder
	{
		object? GetSeriesAccessor(string seriesName);
	}

	public static partial class DataBinder
	{
		// make every effort to construct a schema.
		static ReadOnlyCollection<DbColumn> GetSchema(IDataReader dr)
		{
			if (dr is DbDataReader ddr && ddr.CanGetColumnSchema())
			{
				return ddr.GetColumnSchema();
			}
			else
			{
				DataTable? schemaTable = null;
				try
				{
					// it isn't uncommon for implementations to ignore providing a schema.
					// typically they'll throw NotSupportedException.
					schemaTable = dr.GetSchemaTable();
				}
				catch { }

				if (schemaTable != null) {
					return Schema.FromSchemaTable(schemaTable).GetColumnSchema();
				}
			}
			return Schema.GetWeakSchema(dr).GetColumnSchema();
		}

		//public static IDataBinder<T> CreateDynamic<T>(ReadOnlyCollection<DbColumn> schema, DataBinderOptions? opts = null)
		//{
		//	var bf = ObjectBinder.Get<T>();
		//	var b = bf.Create(schema, opts);
		//	return b;
		//}

		public static IDataBinder<T> Create<T>(ReadOnlyCollection<DbColumn> schema, DataBinderOptions? opts = null)
		{
			opts = opts ?? new DataBinderOptions();
			return new CompiledDataBinder<T>(opts, schema);
		}

		public static IDataBinder<T> Create<T>(IDataReader reader, DataBinderOptions? opts = null)
		{
			var dr = reader.AsDbDataReader();
			opts = opts ?? new DataBinderOptions();
			return CompiledBinderCache<T>.GetBinder(dr, opts);
		}

		public static IDataBinder<T> Create<T>(IDataReader reader, ReadOnlyCollection<DbColumn> logicalSchema, DataBinderOptions? opts = null)
		{
			var dr = reader.AsDbDataReader();
			opts = opts ?? new DataBinderOptions();
			//return CompiledBinderCache<T>.GetBinder(dr, opts);
			return new CompiledDataBinder<T>(opts, GetSchema(dr), logicalSchema);
		}
	}
}
