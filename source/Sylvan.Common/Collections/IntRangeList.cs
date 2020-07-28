using System;
using System.Collections;
using System.Collections.Generic;

namespace Sylvan.Collections
{
	sealed class IntRangeList : IList<int>
	{
		public static IList<int> Range(int start, int count)
		{
			return new IntRangeList(start, count);
		}

		readonly int start;
		readonly int count;

		public IntRangeList(int start, int count)
		{
			this.start = start;
			this.count = count;
		}

		public int this[int index]
		{
			get
			{
				if (index < 0 || index >= count) throw new ArgumentOutOfRangeException(nameof(index));
				return start + index;
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public int Count => this.count;

		public bool IsReadOnly => true;

		public void Add(int item)
		{
			throw new NotSupportedException();
		}

		public void Clear()
		{
			throw new NotSupportedException();
		}

		public bool Contains(int item)
		{
			return item >= start && item < start + count;
		}

		public void CopyTo(int[] array, int arrayIndex)
		{
			if (array == null) throw new ArgumentNullException(nameof(array));
			if (arrayIndex + count > array.Length) throw new ArgumentOutOfRangeException(nameof(arrayIndex));

			for (int i = 0; i < count; i++) {
				array[arrayIndex + i] = start + i;
			}
		}

		public IEnumerator<int> GetEnumerator()
		{
			var end = start + count;
			for (int i = start; i < end; i++) {
				yield return i;
			}
		}

		public int IndexOf(int item)
		{
			var idx = item - start;
			return 
				idx >= 0 && idx < count
				? idx
				: -1;
		}

		public void Insert(int index, int item)
		{
			throw new NotSupportedException();
		}

		public bool Remove(int item)
		{
			throw new NotSupportedException();
		}

		public void RemoveAt(int index)
		{
			throw new NotSupportedException();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}
	}
}
