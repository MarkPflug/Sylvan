using System;
using System.Buffers;
using System.Collections.Concurrent;

namespace Sylvan
{
	/// <summary>
	/// An <see cref="ArrayPool{T}"/> implementation that pools buffers
	/// of a single, fixed size.
	/// </summary>
	public sealed class FixedArrayPool<T> : ArrayPool<T>
	{
		ConcurrentBag<T[]> set;
		int size;

		public FixedArrayPool(int size)
		{
			this.size = size;
			this.set = new ConcurrentBag<T[]>();
		}

		public override T[] Rent(int minimumLength)
		{
			if (minimumLength != size) throw new ArgumentOutOfRangeException(nameof(minimumLength));

			return
				set.TryTake(out T[] array)
				? array
				: new T[size];
		}

		public override void Return(T[] array, bool clearArray = false)
		{
			if (array == null || array.Length != size)
			{
				return;
			}

			if (clearArray)
				Array.Clear(array, 0, array.Length);
			set.Add(array);
		}
	}
}
