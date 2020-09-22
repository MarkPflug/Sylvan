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

	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public sealed class ColumnOrdinalAttribute : Attribute
	{
		public int Ordinal { get; }
		public ColumnOrdinalAttribute(int ordinal)
		{
			this.Ordinal = ordinal;
		}
	}

	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public sealed class ColumnNameAttribute : Attribute
	{
		public string Name { get; }
		public ColumnNameAttribute(string name)
		{
			this.Name = name;
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

	public abstract class DataBinder<T> : IDataBinder<T>
	{
		public static IDataBinder<T> Create(ReadOnlyCollection<DbColumn> schema)
		{
			return new CompiledDataBinder<T>(schema);
		}

		public abstract void Bind(IDataRecord record, T item);
	}
}
