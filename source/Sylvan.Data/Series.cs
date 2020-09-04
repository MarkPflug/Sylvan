using System;
using System.Collections.Generic;
using System.Linq;

namespace Sylvan.Data
{
	sealed class Series
	{
		double[] values;

		public Series(IEnumerable<double> values)
		{
			this.values = values.ToArray();
		}
	}

	public class Series<TK, TV> where TK : IComparable
	{
		TK[] keys;
		TV[] values;

		public Series(TK tk, Func<TK, TK> keyIncrementer, TV[] values)
		{
			this.keys = new TK[values.Length];
			var key = tk;
			for (int i = 0; i < values.Length; i++)
			{
				keys[i] = key;
				key = keyIncrementer(key);
			}

			this.values = values;
		}

		public TV this[TK key]
		{
			get
			{
				if (key.CompareTo(keys[0]) < 0 || key.CompareTo(keys[keys.Length - 1]) > 0)
				{
					throw new IndexOutOfRangeException();
				}
				var idx = Array.BinarySearch(keys, key);
				return values[idx];
			}
		}
	}

	public class DateSeries<T> : Series<DateTime, T>
	{
		public DateSeries(DateTime startDate, T[] values) : base(startDate, d => d.AddDays(1), values) { }

	}
}
