using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Sylvan.Data
{
	public interface ISeries<TK, TV> //: IEnumerable<TV>
		where TK : IComparable<TK>
	{
		TK Minimum { get; }
		TK Maximum { get; }

		TV this[TK key] { get; }
	}

	public sealed class Series<T> : ISeries<int, T>
	{
		public int Minimum { get; }
		public int Maximum => Minimum + values.Length;

		readonly T[] values;

		public Series(int start, IEnumerable<T> values)
		{
			this.Minimum = start;
			this.values = values.ToArray();
		}

		public T this[int key]
		{
			get
			{
				if (key < Minimum || key > Maximum) throw new IndexOutOfRangeException();
				var idx = key - Minimum;
				return values[idx];
			}
		}
	}

	public sealed class DateSeries<T> : ISeries<DateTime, T>, IEnumerable<T>
	{
		public DateTime Minimum { get; }
		public DateTime Maximum { get; }

		readonly T[] values;

		public DateSeries(DateTime startDate, IEnumerable<T> values)
		{
			this.Minimum = startDate.Date;
			this.values = values.ToArray();
			this.Maximum = Minimum.AddDays(this.values.Length);
		}

		public T this[DateTime key]
		{
			get
			{
				if (key < Minimum || key > Maximum) throw new IndexOutOfRangeException();
				var idx = (key - Minimum).Days;
				return values[idx];
			}
		}

		public IEnumerator<T> GetEnumerator()
		{
			foreach (var item in values)
				yield return item;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}
	}
}
