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
		internal static IDataAccessor<string> String = new StringDataAccessor();

		internal static IDataAccessor<int?> NullableInt32 = new NullableInt32DataAccessor();
		internal static IDataAccessor<long?> NullableInt64 = new NullableInt64DataAccessor();
		internal static IDataAccessor<double?> NullableDouble = new NullableDoubleDataAccessor();
		internal static IDataAccessor<decimal?> NullableDecimal = new NullableDecimalDataAccessor();
		internal static IDataAccessor<string?> NullableString = new NullableStringDataAccessor();

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

		sealed class StringDataAccessor : IDataAccessor<string>
		{
			public string Get(IDataRecord r, int ordinal)
			{
				return r.GetString(ordinal);
			}
		}

		sealed class NullableInt32DataAccessor : IDataAccessor<int?>
		{
			public int? Get(IDataRecord r, int ordinal)
			{
				if (r.IsDBNull(ordinal)) return null;
				return r.GetInt32(ordinal);
			}
		}

		sealed class NullableInt64DataAccessor : IDataAccessor<long?>
		{
			public long? Get(IDataRecord r, int ordinal)
			{
				if (r.IsDBNull(ordinal)) return null;
				return r.GetInt64(ordinal);
			}
		}

		sealed class NullableDoubleDataAccessor : IDataAccessor<double?>
		{
			public double? Get(IDataRecord r, int ordinal)
			{
				if (r.IsDBNull(ordinal)) return null;
				return r.GetDouble(ordinal);
			}
		}

		sealed class NullableDecimalDataAccessor : IDataAccessor<decimal?>
		{
			public decimal? Get(IDataRecord r, int ordinal)
			{
				if (r.IsDBNull(ordinal)) return null;
				return r.GetDecimal(ordinal);
			}
		}

		sealed class NullableStringDataAccessor : IDataAccessor<string?>
		{
			public string? Get(IDataRecord r, int ordinal)
			{
				if (r.IsDBNull(ordinal)) return null;
				return r.GetString(ordinal);
			}
		}
	}

	public sealed class DataSeriesColumn<TK>
	{
		public DataSeriesColumn(string name, TK key, int ordinal)
		{
			this.Name = name;
			this.Key = key;
			this.Ordinal = ordinal;
		}

		public string Name { get; }
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

		public TK Minimum { get; }
		public TK Maximum { get; }

		static IDataAccessor<TV> GetAccessor(Type t)
		{
			if (t == typeof(int))
				return (IDataAccessor<TV>)DataAccessor.Int32;
			if (t == typeof(long))
				return (IDataAccessor<TV>)DataAccessor.Int64;
			if (t == typeof(double))
				return (IDataAccessor<TV>)DataAccessor.Double;
			if (t == typeof(decimal))
				return (IDataAccessor<TV>)DataAccessor.Decimal;
			if (t == typeof(string))
				return (IDataAccessor<TV>)DataAccessor.String;

			if (t == typeof(int?))
				return (IDataAccessor<TV>)DataAccessor.Int32;
			if (t == typeof(long?))
				return (IDataAccessor<TV>)DataAccessor.Int64;
			if (t == typeof(double?))
				return (IDataAccessor<TV>)DataAccessor.Double;
			if (t == typeof(decimal?))
				return (IDataAccessor<TV>)DataAccessor.Decimal;

			throw new NotSupportedException();
		}

		public DataSeriesAccessor(IEnumerable<DataSeriesColumn<TK>> columns)
		{
			this.cols = columns.OrderBy(c => c.Key).ToArray();
			this.getter = GetAccessor(typeof(TV));
			this.Minimum = columns.Select(c => c.Key).Min();
			this.Maximum = columns.Select(c => c.Key).Max();
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

		public int ColumnCount => cols.Length;

		public IEnumerable<TK> GetKeys()
		{
			return this.cols.Select(c => c.Key);
		}

		public IEnumerable<TV> GetValues(IDataRecord record)
		{
			for (int i = 0; i < cols.Length; i++)
			{
				var c = cols[i];
				var o = c.Ordinal;
				var value = getter.Get(record, o);
				yield return value;
			}
		}
	}
}
