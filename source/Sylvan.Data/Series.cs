using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Sylvan.Data;

/// <summary>
/// A data series.
/// </summary>
/// <typeparam name="TK">The series key type.</typeparam>
/// <typeparam name="TV">The series value type.</typeparam>
public interface ISeries<TK, TV> : IEnumerable<KeyValuePair<TK, TV>>
	where TK : IComparable<TK>
{
	/// <summary>
	/// Gets the minimum key value in the range.
	/// </summary>
	TK Minimum { get; }

	/// <summary>
	/// Gets the maximum key value in the range.
	/// </summary>
	TK Maximum { get; }

	/// <summary>
	/// Gets the keys of the range.
	/// </summary>
	IReadOnlyList<TK> Keys { get; }

	/// <summary>
	/// Gets the values over the range.
	/// </summary>
	IReadOnlyList<TV> Values { get; }
}

/// <summary>
/// A simple data series.
/// </summary>
public sealed class Series<TK, TV> : ISeries<TK, TV>
	where TK : IComparable<TK>
{
	readonly IReadOnlyList<TK> keys;
	readonly IReadOnlyList<TV> values;

	/// <inheritdoc/>
	public TK Minimum => keys[0];
	
	/// <inheritdoc/>
	public TK Maximum => keys[keys.Count - 1];

	/// <inheritdoc/>
	public IReadOnlyList<TK> Keys => keys;

	/// <inheritdoc/>
	public IReadOnlyList<TV> Values => values;

	/// <summary>
	/// Creates a new Series instance.
	/// </summary>
	public Series(DataSeriesAccessor<TK, TV> seriesData, DbDataReader data)
	{
		this.keys = seriesData.Keys;
		var values = new TV[keys.Count];
		int i = 0;
		foreach (var value in seriesData.ReadValues(data))
		{
			values[i++] = value;
		}
		this.values = values;
	}

	/// <inheritdoc/>
	public IEnumerator<KeyValuePair<TK, TV>> GetEnumerator()
	{
		for (int i = 0; i < keys.Count; i++)
		{
			yield return new KeyValuePair<TK, TV>(keys[i], values[i]);
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return this.GetEnumerator();
	}
}
