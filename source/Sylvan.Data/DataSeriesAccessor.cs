using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Sylvan.Data;

interface IDataAccessor<out T>
{
	T Get(IDataRecord r, int ordinal);
}

static class DataAccessor
{
	internal static IDataAccessor<int> Int32 = new Int32DataAccessor();
	internal static IDataAccessor<long> Int64 = new Int64DataAccessor();
	internal static IDataAccessor<float> Float = new FloatDataAccessor();
	internal static IDataAccessor<double> Double = new DoubleDataAccessor();
	internal static IDataAccessor<decimal> Decimal = new DecimalDataAccessor();
	internal static IDataAccessor<string> String = new StringDataAccessor();

	internal static IDataAccessor<int?> NullableInt32 = new NullableInt32DataAccessor();
	internal static IDataAccessor<long?> NullableInt64 = new NullableInt64DataAccessor();
	internal static IDataAccessor<float?> NullableFloat = new NullableFloatDataAccessor();
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

	sealed class FloatDataAccessor : IDataAccessor<float>
	{
		public float Get(IDataRecord r, int ordinal)
		{
			return r.GetFloat(ordinal);
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

	sealed class NullableFloatDataAccessor : IDataAccessor<float?>
	{
		public float? Get(IDataRecord r, int ordinal)
		{
			if (r.IsDBNull(ordinal)) return null;
			return r.GetFloat(ordinal);
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

/// <summary>
/// A column in a <see cref="DataSeriesAccessor{TK, TV}"/>.
/// </summary>
/// <typeparam name="TK">The type of column key.</typeparam>
public sealed class DataSeriesColumn<TK>
{
	/// <summary>
	/// Constructs a new DataSeriesColumn.
	/// </summary>
	public DataSeriesColumn(string name, TK key, int ordinal)
	{
		this.Name = name;
		this.Key = key;
		this.Ordinal = ordinal;
	}

	/// <summary>
	/// The physical column name.
	/// </summary>
	public string Name { get; }

	/// <summary>
	/// The key value of the column.
	/// </summary>
	public TK Key { get; }

	/// <summary>
	/// The physical column ordinal.
	/// </summary>
	public int Ordinal { get; }
}

/// <summary>
/// Allows access to the range and values of keys in a data series.
/// </summary>
/// <typeparam name="TK"></typeparam>
public interface IDataSeriesRange<TK> : IReadOnlyList<TK>
{
	/// <summary>
	/// Gets the keys of the series.
	/// </summary>
	IReadOnlyList<TK> Keys { get; }

	/// <summary>
	/// Gets the minimum key value.
	/// </summary>
	TK Minimum { get; }

	/// <summary>
	/// Gets the maximum key value.
	/// </summary>
	TK Maximum { get; }
}

/// <summary>
/// Reads a series of columns from a DbDataReader that all have the same type.
/// </summary>
public sealed class DataSeriesAccessor<TK, TV>
	: IDataSeriesRange<TK>
	where TK : IComparable<TK>
{
	//readonly bool allowNull;
	readonly DataSeriesColumn<TK>[] cols;
	readonly IDataAccessor<TV> getter;

	readonly TK[] keys;

	/// <summary>
	/// Gets an ordered list of the series column keys.
	/// </summary>
	public IReadOnlyList<TK> Keys => keys;

	/// <inheritdoc/>
	public TK Minimum => keys[0];
	/// <inheritdoc/>
	public TK Maximum => keys[keys.Length - 1];

	/// <summary>
	/// Gets the number of elements in the series.
	/// </summary>
	public int Count => keys.Length;

	/// <summary>
	/// Gets the key at the given index.
	/// </summary>
	public TK this[int index] => keys[index];

	static IDataAccessor<TV> GetAccessor(Type t)
	{
		if (t == typeof(int))
			return (IDataAccessor<TV>)DataAccessor.Int32;
		if (t == typeof(long))
			return (IDataAccessor<TV>)DataAccessor.Int64;
		if (t == typeof(float))
			return (IDataAccessor<TV>)DataAccessor.Float;
		if (t == typeof(double))
			return (IDataAccessor<TV>)DataAccessor.Double;
		if (t == typeof(decimal))
			return (IDataAccessor<TV>)DataAccessor.Decimal;
		if (t == typeof(string))
			return (IDataAccessor<TV>)DataAccessor.String;

		if (t == typeof(int?))
			return (IDataAccessor<TV>)DataAccessor.NullableInt32;
		if (t == typeof(long?))
			return (IDataAccessor<TV>)DataAccessor.NullableInt64;
		if (t == typeof(float?))
			return (IDataAccessor<TV>)DataAccessor.NullableFloat;
		if (t == typeof(double?))
			return (IDataAccessor<TV>)DataAccessor.NullableDouble;
		if (t == typeof(decimal?))
			return (IDataAccessor<TV>)DataAccessor.NullableDecimal;

		throw new NotSupportedException();
	}

	/// <summary>
	/// Creates a new DataSeriesAccessor.
	/// </summary>
	public DataSeriesAccessor(IEnumerable<DataSeriesColumn<TK>> columns)
	{
		this.cols = columns.OrderBy(c => c.Key).ToArray();
		this.keys = cols.Select(c => c.Key).ToArray();
		this.getter = GetAccessor(typeof(TV));
	}

	//public IEnumerable<KeyValuePair<TK, TV>> GetSeries(IDataRecord record)
	//{
	//	for (int i = 0; i < cols.Length; i++)
	//	{
	//		var c = cols[i];
	//		var o = c.Ordinal;
	//		var value = getter.Get(record, o);
	//		yield return new KeyValuePair<TK, TV>(c.Key, value);
	//	}
	//}

	//public IEnumerable<TK> GetKeys()
	//{
	//	return this.cols.Select(c => c.Key);
	//}

	/// <summary>
	/// Reads the column values from the data record.
	/// </summary>
	/// <param name="record">The record to read the values from.</param>
	/// <returns>The sequence of values ordered by corresponding series column key.</returns>
	public IEnumerable<TV> ReadValues(IDataRecord record)
	{
		for (int i = 0; i < cols.Length; i++)
		{
			var c = cols[i];
			var o = c.Ordinal;
			var value = getter.Get(record, o);
			yield return value;
		}
	}

	/// <summary>
	/// Enumerates the the keys in the series.
	/// </summary>
	/// <returns></returns>
	public IEnumerator<TK> GetEnumerator()
	{
		return this.Keys.GetEnumerator();
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return this.GetEnumerator();
	}
}
