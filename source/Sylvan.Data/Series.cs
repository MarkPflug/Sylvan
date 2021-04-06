using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace Sylvan.Data
{
	public interface ISeries<TK, TV> : IEnumerable<KeyValuePair<TK, TV>>
		where TK : IComparable<TK>
	{
		TK Minimum { get; }
		TK Maximum { get; }

		IReadOnlyList<TK> Keys { get; }
		IReadOnlyList<TV> Values { get; }
	}

	public sealed class Series<TK, TV> : ISeries<TK,TV>
		where TK : IComparable<TK>
	{
		readonly IReadOnlyList<TK> keys;
		readonly IReadOnlyList<TV> values;

		public TK Minimum => keys[0];
		public TK Maximum => keys[keys.Count - 1];

		public IReadOnlyList<TK> Keys => keys;
		public IReadOnlyList<TV> Values => values;

		public Series(DataSeriesAccessor<TK,TV> seriesData, IDataRecord data)
		{
			this.keys = seriesData.Keys;
			var values = new TV[keys.Count];
			int i = 0;
			foreach(var value in seriesData.ReadValues(data))
			{
				values[i++] = value;
			}
			this.values = values;
		}

		public IEnumerator<KeyValuePair<TK, TV>> GetEnumerator()
		{
			for(int i = 0; i < keys.Count; i++)
			{
				yield return new KeyValuePair<TK, TV>(keys[i], values[i]);
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}
	}
}
