using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Sylvan.Data
{
	public interface ISeries<TK, TV> : IEnumerable<KeyValuePair<TK, TV>>
		where TK : IComparable<TK>
	{
		TK Minimum { get; }
		TK Maximum { get; }

		TV this[TK key] { get; }
	}

	public sealed class Series<T> : ISeries<int, T>
	{
		public int Minimum { get; }
		public int Step { get; }
		public int Maximum { get; }

		readonly T[] values;

		public Series(int start, IEnumerable<T> values) : this(start, 1, values)
		{

		}

		public Series(int start, int step, IEnumerable<T> values)
		{
			this.Minimum = start;
			this.Step = step;
			this.values = values.ToArray();
			this.Maximum = Minimum + this.values.Length * Step;
		}

		public T this[int key]
		{
			get
			{
				if (key < Minimum || key > Maximum) throw new IndexOutOfRangeException();
				var idx = (key - Minimum) / Step;

				return values[idx];
			}
		}

		public IEnumerator<KeyValuePair<int, T>> GetEnumerator()
		{
			int key = Minimum;
			for (int i = 0; i < values.Length; i++, key += Step)
			{
				yield return new KeyValuePair<int, T>(key, values[i]);
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}
	}

	public sealed class DateSeries<T> : ISeries<DateTime, T>
	{
		public DateTime Minimum { get; }
		public TimeSpan Step { get; }
		public DateTime Maximum { get; }

		readonly T[] values;

		public DateSeries(DateTime startDate, IEnumerable<T> values)
			: this(startDate, TimeSpan.FromDays(1), values)
		{
		}

		public DateSeries(DateTime startDate, TimeSpan step, IEnumerable<T> values)
		{
			this.Minimum = startDate.Date;
			this.Step = step;
			this.values = values.ToArray();
			this.Maximum = Minimum.AddDays(this.values.Length);
		}

		public T this[DateTime key]
		{
			get
			{
				if (key < Minimum || key > Maximum) throw new IndexOutOfRangeException();
				var idx = (key.Ticks - Minimum.Ticks) / Step.Ticks;
				return values[idx];
			}
		}

		public IEnumerator<KeyValuePair<DateTime, T>> GetEnumerator()
		{
			DateTime key = Minimum;
			for (int i = 0; i < this.values.Length; i++)
			{
				yield return new KeyValuePair<DateTime, T>(key, values[i]);
				key = key.Add(Step);
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}
	}
}
