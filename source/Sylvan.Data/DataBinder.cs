using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{

	public static class DataBinderFactory
	{
		public static IDataBinder<T> Create<T>(this IDataBinderFactory<T> factory, DbDataReader reader)
		{
			var schema = reader.GetColumnSchema();
			return factory.Create(schema);
		}
	}

	public interface IDataBinderFactory<T>
	{
		IDataBinder<T> Create(IReadOnlyList<DbColumn> schema);
	}

	public interface IDataBinder<T>
	{
		void Bind(IDataRecord record, T item);
	}

	interface IDataSeriesBinder
	{
		object? GetSeriesAccessor(string seriesName);
	}

	public static partial class DataBinder
	{

		public static IDataBinderFactory<T> CreateFactory<T>()
		{
			return new DynamicDataBinderFactory<T>();
		}


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

		public static IDataBinder<T> Create<T>(ReadOnlyCollection<DbColumn> schema, DataBinderOptions? opts = null)
		{
			opts = opts ?? new DataBinderOptions();
			return new CompiledDataBinder<T>(opts, schema);
		}

		public static IDataBinder<T> Create<T>(IDataReader dr, DataBinderOptions? opts = null)
		{
			opts = opts ?? new DataBinderOptions();
			return new CompiledDataBinder<T>(opts, GetSchema(dr));
		}

		public static IDataBinder<T> Create<T>(IDataReader dr, ReadOnlyCollection<DbColumn> logicalSchema, DataBinderOptions? opts = null)
		{
			opts = opts ?? new DataBinderOptions();
			return new CompiledDataBinder<T>(opts, GetSchema(dr), logicalSchema);
		}
	}
}
