using System;
using System.Runtime.CompilerServices;

namespace Sylvan
{
	/// <summary>
	/// An IStringPool implementation.
	/// </summary>
	public sealed partial class StringPool : IStringPool
	{
		const int DefaultCapacity = 64;
		const int DefaultSizeLimit = 32;
		const int CollisionLimit = 8;

		// This is a greatly-simplified HashSet<string> that only allows additions.
		// and accepts char[] instead of string.

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

		readonly int stringSizeLimit;
		int[] buckets;
		Entry[] entries;

		int count;

		/// <summary>
		/// Creates a new StringPool instance.
		/// </summary>
		public StringPool() : this(DefaultSizeLimit) { }

		/// <summary>
		/// Creates a new StringPool instance.
		/// </summary>
		/// <param name="stringSizeLimit">The size limit beyond which strings will not be pooled.</param>
		/// <remarks>
		/// The <paramref name="stringSizeLimit"/> prevents pooling strings beyond a certain size. 
		/// Longer strings have a higher likelihood of being unique, and carry extra cost for identifying uniqueness.
		/// This limit allows not spending time identifying duplicates for such strings.
		/// </remarks>
		public StringPool(int stringSizeLimit)
		{
			int size = GetSize(DefaultCapacity);
			this.stringSizeLimit = stringSizeLimit;
			this.buckets = new int[size];
			this.entries = new Entry[size];
		}

		static int GetSize(int capacity)
		{
			var size = DefaultCapacity;
			while (size < capacity)
				size = size * 2;
			return size;
		}

		/// <summary>
		/// Gets a string containing the characters in the input buffer.
		/// </summary>
		public string? GetString(char[] buffer, int offset, int length)
		{
			if (buffer == null) throw new ArgumentNullException(nameof(buffer));
			if (length == 0) return string.Empty;
			if (length > stringSizeLimit) return null;

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
					e.count++;
					return e.str;
				}

				i = e.next;

				collisionCount++;
				if (collisionCount > CollisionLimit)
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

			ref Entry entry = ref entries![index];
			entry.hashCode = hashCode;
			entry.count = 1;
			entry.next = bucket - 1;
			entry.str = stringValue;

			bucket = index + 1; // bucket is an int ref

			return stringValue;
		}

		Entry[] Resize()
		{
			var newSize = GetSize(this.count + 1);

			var entries = new Entry[newSize];

			int count = this.count;
			Array.Copy(this.entries, entries, count);

			this.buckets = new int[newSize];

			for (int i = 0; i < count; i++)
			{
				if (entries[i].next >= -1)
				{
					ref int bucket = ref GetBucket(entries[i].hashCode);
					entries[i].next = bucket - 1;
					bucket = i + 1;
				}
			}

			this.entries = entries;
			return entries;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		ref int GetBucket(uint hashCode)
		{
			int[] buckets = this.buckets;
			return ref buckets[hashCode & ((uint)buckets.Length - 1)];
		}

		struct Entry
		{
			public uint hashCode;
			public int next;
			public int count;
			public string str;
		}
	}
}
