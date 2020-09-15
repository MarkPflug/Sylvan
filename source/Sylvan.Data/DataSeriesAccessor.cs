using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Sylvan.Data
{
	interface IDataAccessor<out T>
	{
		T Get(IDataRecord r, int ordinal);
	}

	static class DataAccessor
	{
		internal static IDataAccessor<int> Int32 = new Int32DataAccessor();
		internal static IDataAccessor<long> Int64 = new Int64DataAccessor();
		internal static IDataAccessor<double> Double = new DoubleDataAccessor();
		internal static IDataAccessor<decimal> Decimal = new DecimalDataAccessor();

		sealed class Int32DataAccessor : IDataAccessor<int>
		{
			public int Get(IDataRecord r, int ordinal)
			{
				return r.GetInt32(ordinal);
			}
		}

		sealed class Int64DataAccessor : IDataAccessor<long>
		{
			public long Get(IDataRecord r, int ordinal)
			{
				return r.GetInt64(ordinal);
			}
		}

		sealed class DoubleDataAccessor : IDataAccessor<double>
		{
			public double Get(IDataRecord r, int ordinal)
			{
				return r.GetDouble(ordinal);
			}
		}

		sealed class DecimalDataAccessor : IDataAccessor<decimal>
		{
			public decimal Get(IDataRecord r, int ordinal)
			{
				return r.GetDecimal(ordinal);
			}
		}
	}

	public class DataSeriesColumn<TK>
	{
		public DataSeriesColumn(TK key, int ordinal)
		{
			this.Key = key;
			this.Ordinal = ordinal;
		}

		public TK Key { get; }
		public int Ordinal { get; }
	}

	/// <summary>
	/// Reads a series of columns from a DbDataReader as an enumerable.
	/// </summary>
	public sealed class DataSeriesAccessor<TK, TV> where TK : IComparable<TK>
	{
		//readonly bool allowNull;
		readonly DataSeriesColumn<TK>[] cols;
		readonly IDataAccessor<TV> getter;

		static IDataAccessor<TV> GetAccessor(Type t)
		{
			if (t == typeof(int))
				return (IDataAccessor<TV>)DataAccessor.Int32;

			throw new NotSupportedException();
		}

		public DataSeriesAccessor(IEnumerable<DataSeriesColumn<TK>> columns)
		{
			this.cols = columns.OrderBy(c => c.Key).ToArray();
			this.getter = GetAccessor(typeof(TV));
		}

		public IEnumerable<KeyValuePair<TK, TV>> GetSeries(IDataRecord record)
		{
			for (int i = 0; i < cols.Length; i++)
			{
				var c = cols[i];
				var o = c.Ordinal;
				var value = getter.Get(record, o);
				yield return new KeyValuePair<TK, TV>(c.Key, value);
			}
		}
	}
}
