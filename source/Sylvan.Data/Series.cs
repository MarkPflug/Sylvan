using System;
using System.Collections.Generic;
using System.Linq;

namespace Sylvan.Data
{
	public interface ISeries<TK, TV> //: IEnumerable<TV>
		where TK : IComparable<TK>
	{
		TK Start { get; }
		TK End { get; }

		TV this[TK key] { get; }
	}

	public sealed class Series<T> : ISeries<int, T>
	{
		public int Start { get; }
		public int End => Start + values.Length;

		readonly T[] values;

		public Series(int start, IEnumerable<T> values)
		{
			this.Start = start;
			this.values = values.ToArray();
		}

		public T this[int key]
		{
			get
			{
				if (key < Start || key > End) throw new IndexOutOfRangeException();
				var idx = key - Start;
				return values[idx];
			}
		}
	}

	public class DateSeries<T> : ISeries<DateTime, T>
	{
		public DateTime Start { get; }

		public DateTime End { get; }

		readonly T[] values;

		public DateSeries(DateTime startDate, IEnumerable<T> values)
		{
			this.Start = startDate.Date;
			this.values = values.ToArray();
			this.End = Start.AddDays(this.values.Length);
		}

		public T this[DateTime key]
		{
			get
			{
				if (key < Start || key > End) throw new IndexOutOfRangeException();
				var idx = (key - Start).Days;
				return values[idx];
			}
		}
	}
}
