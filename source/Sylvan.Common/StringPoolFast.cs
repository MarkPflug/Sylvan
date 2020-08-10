using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sylvan
{
	sealed partial class StringPoolFast
	{
		// This is a greatly-simplified HashSet<string> that only allows additions.
		// and accepts ReadOnlySpan<char> instead of string.

		// An extremely simple, and hopefully fast, hash algorithm.
		static uint GetHashCode(char[] buffer, int offset, int length)
		{
			uint hash = 0;
			var o = offset;
			for (int i = 0; i < length; i++)
			{
				hash = hash * 31 + buffer[offset++];
			}
			return hash;
		}

		bool checkIntern;
		int[] buckets;
		Entry[] entries;

		int count;

		/// <summary>
		/// 
		/// </summary>
		public StringPoolFast() : this(32, false)
		{

		}
		/// <summary>
		/// 
		/// </summary>
		/// <param name="capacity"></param>
		public StringPoolFast(int capacity) : this(capacity, false)
		{

		}
		/// <summary>
		/// 
		/// </summary>
		/// <param name="capacity"></param>
		/// <param name="checkIntern"></param>
		public StringPoolFast(int capacity, bool checkIntern)
		{
			int size = GetSize(capacity);
			this.buckets = new int[size];
			this.entries = new Entry[size];
			this.checkIntern = checkIntern;

#if TARGET_64BIT
            this.fastModMultiplier = GetFastModMultiplier((uint)size);
#endif
		}

		static int GetSize(int capacity)
		{
			var size = 16;
			while (size < capacity)
				size = size * 2;
			return size;
		}
		/// <summary>
		/// 
		/// </summary>
		public string? GetString(char[] buffer, int offset, int length)
		{
			if (buffer == null) throw new ArgumentNullException(nameof(buffer));
			if (length == 0) return string.Empty;

			var entries = this.entries;
			var hashCode = GetHashCode(buffer, offset, length);

			uint collisionCount = 0;
			ref int bucket = ref GetBucket(hashCode);
			int i = bucket - 1; // Value in _buckets is 1-based

			while ((uint)i < (uint)entries.Length)
			{
				ref var e = ref entries[i];
				if (e.hashCode == hashCode && MemoryExtensions.Equals(buffer.AsSpan().Slice(offset, length), e.str.AsSpan(), StringComparison.Ordinal))
				{
					return e.str;
				}

				i = e.next;

				collisionCount++;
				if (collisionCount > (uint)entries.Length)
				{
					// protects against malicious inputs
					return null;
				}
			}

			int count = this.count;
			if (count == entries.Length)
			{
				entries = Resize();
				bucket = ref GetBucket(hashCode);
			}
			int index = count;
			this.count = count + 1;

			var stringValue = new string(buffer, offset, length);
			if (checkIntern)
				stringValue = string.IsInterned(stringValue);
			ref Entry entry = ref entries![index];
			entry.hashCode = hashCode;
			entry.next = bucket - 1; // Value in _buckets is 1-based
			entry.str = stringValue;

			bucket = index + 1; // bucket is a ref

			return stringValue;
		}

		Entry[] Resize()
		{
			var newSize = GetSize(this.count + 1);

			Debug.Assert(newSize >= this.entries.Length);

			var entries = new Entry[newSize];

			int count = this.count;
			Array.Copy(this.entries, entries, count);

			// Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
			this.buckets = new int[newSize];

			for (int i = 0; i < count; i++)
			{
				if (entries[i].next >= -1)
				{
					ref int bucket = ref GetBucket(entries[i].hashCode);
					entries[i].next = bucket - 1; // Value in _buckets is 1-based
					bucket = i + 1;
				}
			}

			this.entries = entries;
			return entries;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private ref int GetBucket(uint hashCode)
		{
			int[] buckets = this.buckets!;
			return ref buckets[hashCode & ((uint)buckets.Length - 1)];
		}

		struct Entry
		{
			public uint hashCode;
			/// <summary>
			/// 0-based index of next entry in chain: -1 means end of chain
			/// also encodes whether this entry _itself_ is part of the free list by changing sign and subtracting 3,
			/// so -2 means end of free list, -3 means index 0 but on free list, -4 means index 1 but on free list, etc.
			/// </summary>
			public int next;
			public string str;
		}
	}
}
