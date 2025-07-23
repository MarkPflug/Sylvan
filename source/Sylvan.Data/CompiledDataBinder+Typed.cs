using System;
using System.Collections.ObjectModel;
using System.Data.Common;

namespace Sylvan.Data;

sealed class CompiledDataBinder<T>
	: IDataBinder<T>, IDataSeriesBinder
	where T : class
{
	object? IDataSeriesBinder.GetSeriesAccessor(string seriesName)
	{
		return ((IDataSeriesBinder)this.compiledDataBinder).GetSeriesAccessor(seriesName);
	}

	readonly CompiledDataBinder compiledDataBinder;

	internal CompiledDataBinder(
		DataBinderOptions opts,
		ReadOnlyCollection<DbColumn> physicalSchema
	)
		: this(opts, physicalSchema, physicalSchema)
	{
	}

	internal CompiledDataBinder(
		DataBinderOptions opts,
		ReadOnlyCollection<DbColumn> physicalSchema,
		ReadOnlyCollection<DbColumn> logicalSchema
	)
	{
		this.compiledDataBinder = new CompiledDataBinder(opts, physicalSchema, logicalSchema, typeof(T));
	}

	void IDataBinder<T>.Bind(DbDataReader record, T item)
	{
		((IDataBinder)this.compiledDataBinder).Bind(record, item);
	}

	void IDataBinder.Bind(DbDataReader record, object item)
	{
		if (item is T t)
		{
			((IDataBinder<T>)this).Bind(record, t);
		}
		else
		{
			throw new ArgumentException();
		}
	}
}