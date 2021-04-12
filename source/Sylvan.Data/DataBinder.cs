using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Globalization;

namespace Sylvan.Data
{
	public static class DataBinderExtensions
	{
		public static T GetRecord<T>(this IDataBinder<T> binder, IDataRecord record, Func<IDataRecord, Exception, bool>? errorHandler = null) where T : new()
		{
			var t = new T();
			try
			{
				binder.Bind(record, t);
			}
			catch (Exception e) when (errorHandler != null)
			{
				if (!errorHandler(record, e))
				{
					throw;
				}
			}
			return t;
		}
	}

	public interface IDataBinder<T>
	{
		void Bind(IDataRecord record, T item);
	}

	public abstract class BinderFactory<T>
	{
		public abstract IDataBinder<T> CreateBinder(ReadOnlyCollection<DbColumn> schema);
	}

	public sealed class DataBinderOptions
	{
		internal static readonly DataBinderOptions Default = new DataBinderOptions();
		//public bool ReaderAllowsDynamicAccess { get; set; }
		public CultureInfo Culture { get; set; }

		/// <summary>
		/// Indicates how the data source will bind to the target type.
		/// Defaults to <see cref="DataBindingMode.AllProperties"/> which requires that
		/// the datasource have column that binds to each property, but would allow unbound columns.
		/// </summary>
		public DataBindingMode BindingMode { get; set; }

		public DataBinderOptions()
		{
			this.Culture = CultureInfo.InvariantCulture;
			this.BindingMode = DataBindingMode.AllProperties;
		}
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
