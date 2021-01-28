using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public sealed class ColumnSeriesAttribute : Attribute
	{
		public string? SeriesPattern { get; }

		public ColumnSeriesAttribute() { }

		public ColumnSeriesAttribute(string seriesPattern)
		{
			this.SeriesPattern = seriesPattern;
		}
	}

	public static class DataBinderExtensions
	{
		public static T GetRecord<T>(this IDataBinder<T> binder, IDataRecord record) where T : new()
		{
			var t = new T();
			binder.Bind(record, t);
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

	public abstract class DataBinder<T> : IDataBinder<T>
	{
		public static IDataBinder<T> Create(ReadOnlyCollection<DbColumn> physicalSchema)
		{
			return CreateFactory(physicalSchema).CreateBinder(physicalSchema);
		}

		public static IDataBinder<T> Create(ReadOnlyCollection<DbColumn> logicalSchema, ReadOnlyCollection<DbColumn> physicalSchema)
		{
			return CreateFactory(logicalSchema).CreateBinder(physicalSchema);
		}

		public static BinderFactory<T> CreateFactory(ReadOnlyCollection<DbColumn> logicalSchema)
		{
			return new CompiledDataBinderFactory<T>(logicalSchema);
		}

		public abstract void Bind(IDataRecord record, T item);
	}
}
