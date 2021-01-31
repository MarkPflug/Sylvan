using System;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Globalization;

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

#warning how do I name this?
		// TODO: should this even exist? or should I have a way to auto-map the schema?
		public Func<string, int, string?>? ColumnNamer { get; set; }

		public DataBindMode BindMode { get; set; }

		public DataBinderOptions()
		{
			this.Culture = CultureInfo.InvariantCulture;
			this.ColumnNamer = DataBinder.DefaultNameMapping;
			this.BindMode = DataBindMode.Neither;
		}
	}

	public static partial class DataBinder
	{
		public static IDataBinder<T> Create<T>(IDataReader dr, DataBinderOptions? opts = null)
		{
			ReadOnlyCollection<DbColumn>? schema = null;
			if (dr is DbDataReader ddr && ddr.CanGetColumnSchema())
			{
				schema = ddr.GetColumnSchema();
			}
			else
			{
				throw new NotImplementedException();
			}

			opts = opts ?? new DataBinderOptions();
			return new CompiledDataBinder<T>(opts, schema);
		}

		public static IDataBinder<T> Create<T>(IDataReader dr, ReadOnlyCollection<DbColumn> logicalSchema, DataBinderOptions? opts = null)
		{
			ReadOnlyCollection<DbColumn>? schema = null;
			if (dr is DbDataReader ddr && ddr.CanGetColumnSchema())
			{
				schema = ddr.GetColumnSchema();
			}
			else
			{
				throw new NotImplementedException();
			}

			opts = opts ?? new DataBinderOptions();
			return new CompiledDataBinder<T>(opts, schema, logicalSchema);
		}
	}
}
