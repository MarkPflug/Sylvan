using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sylvan;

/// <summary>
/// An IStringFactory implementation that provides string de-duping capabilities..
/// </summary>
public sealed partial class StringPool
{
	const int DefaultCapacity = 64;
	const int DefaultSizeLimit = 32;
	const int CollisionLimit = 8;

	// This is a greatly-simplified HashSet<string> that only allows additions.
	// and accepts char[] instead of string.

	// An extremely simple, and hopefully fast, hash algorithm.
	static uint GetHashCode(ReadOnlySpan<char> buffer, int offset, int length)
	{
		uint hash = 0;
		for (int i = 0; i < length; i++)
		{
			hash = hash * 31 + buffer[offset++];
		}
		return hash;
	}

	IEnumerable<(string str, int count)> GetUsage()
	{
		for (int i = 0; i < this.buckets.Length; i++)
		{
			var b = buckets[i];
			if (b != 0)
			{
				var idx = b - 1;
				while ((uint)idx < entries.Length)
				{
					var e = this.entries[idx];
					yield return (e.str, e.count);
					idx = e.next;
				}
			}
		}
	}

	readonly int stringSizeLimit;
	int[] buckets; // contains index into entries offset by -1. So that 0 (default) means empty bucket.
	Entry[] entries;

	int count;
	long uniqueLen;
	long dupeLen;
	long skipLen;

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
	/// Longer strings are typically less likely to be duplicated, and and carry extra cost for identifying uniqueness.
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
	public string GetString(char[] buffer, int offset, int length)
	{
		return GetString(buffer.AsSpan().Slice(offset, length));
	}

	/// <summary>
	/// Gets a string containing the characters in the input buffer.
	/// </summary>
	public string GetString(ReadOnlySpan<char> buffer)
	{
		var length = buffer.Length;

		if (buffer == null) throw new ArgumentNullException(nameof(buffer));
		if (length == 0) return string.Empty;
		if (length > stringSizeLimit)
		{
			this.skipLen += length;
#if !NETSTANDARD2_0
			return new string(buffer);
#else
			return GetStringUnsafe(buffer);
#endif
		}

		var entries = this.entries;
		var hashCode = GetHashCode(buffer, 0, length);

		uint collisionCount = 0;
		ref int bucket = ref GetBucket(hashCode);
		int i = bucket - 1;

		while ((uint)i < (uint)entries.Length)
		{
			ref var e = ref entries[i];
			if (e.hashCode == hashCode && MemoryExtensions.Equals(buffer, e.str.AsSpan(), StringComparison.Ordinal))
			{
				e.count++;
				this.dupeLen += length;
				return e.str;
			}

			i = e.next;

			collisionCount++;
			if (collisionCount > CollisionLimit)
			{
				// protects against malicious inputs
				// too many collisions give up and let the caller create the string.					
#if !NETSTANDARD2_0
				return new string(buffer);
#else
				return GetStringUnsafe(buffer);
#endif
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

		var stringValue =
#if !NETSTANDARD2_0
				 new string(buffer);
#else
				 GetStringUnsafe(buffer);
#endif

		ref Entry entry = ref entries![index];
		this.uniqueLen += length;
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

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static unsafe string GetStringUnsafe(ReadOnlySpan<char> buffer)
	{
		fixed (char* pValue = &buffer.GetPinnableReference())
		{
			return new string(pValue, 0, buffer.Length);
		}
	}

	struct Entry
	{
		public uint hashCode;
		public int next;
		public int count;
		public string str;
	}
}
