using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data;

/// <summary>
/// An interface that defines the ability to bind a DbDataReader to an object.
/// </summary>
public interface IDataBinder
{
	/// <summary>
	/// Binds a data record to an object.
	/// </summary>
	/// <param name="record">The data reader.</param>
	/// <param name="item">The item to bind to.</param>
	void Bind(DbDataReader record, object item);
}

/// <summary>
/// An interface that defines the ability to bind a DbDataReader to an object.
/// </summary>
public interface IDataBinder<T> : IDataBinder
{
	/// <summary>
	/// Binds a data record to an object.
	/// </summary>
	/// <param name="record">The data reader.</param>
	/// <param name="item">The item to bind to.</param>
	void Bind(DbDataReader record, T item);
}

interface IDataSeriesBinder
{
	object? GetSeriesAccessor(string seriesName);
}

/// <summary>
/// Defines methods to create a general purpose data binder.
/// </summary>
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

			if (schemaTable != null)
			{
				return Schema.FromSchemaTable(schemaTable).GetColumnSchema();
			}
		}
		return Schema.GetWeakSchema(dr).GetColumnSchema();
	}
	
	/// <summary>
	/// Creates a data binder.
	/// </summary>
	/// <param name="schema">The schema of incoming data.</param>
	/// <param name="recordType">The type of record to bind to.</param>
	/// <param name="opts">The binding options.</param>
	/// <returns>An IDataBinder instance.</returns>
	public static IDataBinder Create(ReadOnlyCollection<DbColumn> schema, Type recordType, DataBinderOptions? opts = null)
	{
		opts ??= new DataBinderOptions();
		return new CompiledDataBinder(opts, schema, recordType);
	}

	/// <summary>
	/// Creates a data binder.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="schema">The schema of incoming data.</param>
	/// <param name="opts">The binding options.</param>
	/// <returns>An IDataBinder{T} instance.</returns>
	public static IDataBinder<T> Create<T>(ReadOnlyCollection<DbColumn> schema, DataBinderOptions? opts = null)
		where T : class
	{
		opts ??= new DataBinderOptions();
		return new CompiledDataBinder<T>(opts, schema);
	}
	
	/// <summary>
	/// Creates a data binder.
	/// </summary>
	/// <param name="reader">A data reader.</param>
	/// <param name="recordType">The type of record to bind to.</param>
	/// <param name="opts">The binding options.</param>
	/// <returns>An IDataBinder instance.</returns>
	public static IDataBinder Create(IDataReader reader, Type recordType, DataBinderOptions? opts = null)
	{
		var dr = reader.AsDbDataReader();
		opts ??= new DataBinderOptions();
		return CompiledBinderCache.GetBinder(dr, opts, recordType);
	}

	/// <summary>
	/// Creates a data binder.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="reader">A data reader.</param>
	/// <param name="opts">The binding options.</param>
	/// <returns>An IDataBinder{T} instance.</returns>
	public static IDataBinder<T> Create<T>(IDataReader reader, DataBinderOptions? opts = null)
		where T : class
	{
		var dr = reader.AsDbDataReader();
		opts ??= new DataBinderOptions();
		return CompiledBinderCache<T>.GetBinder(dr, opts);
	}

	/// <summary>
	/// Creates a data binder.
	/// </summary>
	/// <param name="reader">A data reader.</param>
	/// <param name="schema">The schema of the incoming data.</param>
	/// <param name="recordType">The type of record to bind to.</param>
	/// <param name="opts">The binding options.</param>
	/// <returns>An IDataBinder{T} instance.</returns>
	public static IDataBinder Create(IDataReader reader, ReadOnlyCollection<DbColumn> schema, Type recordType, DataBinderOptions? opts = null)
	{
		var dr = reader.AsDbDataReader();
		opts ??= new DataBinderOptions();
		return new CompiledDataBinder(opts, GetSchema(dr), schema, recordType);
	}
	
	/// <summary>
	/// Creates a data binder.
	/// </summary>
	/// <typeparam name="T">The type of record to bind to.</typeparam>
	/// <param name="reader">A data reader.</param>
	/// <param name="schema">The schema of the incoming data.</param>
	/// <param name="opts">The binding options.</param>
	/// <returns>An IDataBinder{T} instance.</returns>
	public static IDataBinder<T> Create<T>(IDataReader reader, ReadOnlyCollection<DbColumn> schema, DataBinderOptions? opts = null)
		where T : class
	{
		var dr = reader.AsDbDataReader();
		opts ??= new DataBinderOptions();
		return new CompiledDataBinder<T>(opts, GetSchema(dr), schema);
	}

	/// <summary>
	/// Binds the current record to a new object instance.
	/// </summary>
	public static object GetRecord(this IDataBinder binder, DbDataReader record, Type recordType)
	{
		var item = Activator.CreateInstance(recordType)!;
		binder.Bind(record, item);
		return item;
	}
	
	/// <summary>
	/// Binds the current record to a new object instance.
	/// </summary>
	public static T GetRecord<T>(this IDataBinder<T> binder, DbDataReader record)
		where T : new()
	{
		var item = new T();
		binder.Bind(record, item);
		return item;
	}

	/// <summary>
	/// Binds the current record to a new object instance.
	/// </summary>
	public static T GetRecord<T>(this IDataBinder<T> binder, DbDataReader record, Func<IDataRecord, T, Exception, bool>? errorHandler = null)
		where T : new()
	{
		var item = new T();
		try
		{
			binder.Bind(record, item);
		}
		catch (Exception e) when (errorHandler != null)
		{
			if (!errorHandler(record, item, e))
			{
				throw;
			}
		}
		return item;
	}
	
	/// <summary>
	/// Binds the current record to a new object instance.
	/// </summary>
	public static object GetRecord(this IDataBinder binder, DbDataReader record, Type recordType, Func<IDataRecord, object, Exception, bool>? errorHandler = null)
	{
		var item = Activator.CreateInstance(recordType)!;
		try
		{
			binder.Bind(record, item);
		}
		catch (Exception e) when (errorHandler != null)
		{
			if (!errorHandler(record, item, e))
			{
				throw;
			}
		}
		return item;
	}
}
