#if NET6_0_OR_GREATER

using System;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Sylvan;

/// <summary>
/// Provides support for de-duping strings to minimize allocation.
/// CachingStringPool is optimized for scenarios where the same string might be requested repeatedly for
/// the same data column.
/// </summary>
public sealed class CachingStringPool
{
	const int DefaultCapacity = 64;
	const int DefaultSizeLimit = 32;
	const int CollisionLimit = 8;

	// This is a greatly-simplified HashSet<string> that only allows additions.
	// and accepts char[] instead of string.

	// An extremely simple, and hopefully fast, hash algorithm.
	static uint GetHashCode(ReadOnlySpan<char> buffer)
	{
		uint hash = 0;
		for (int i = 0; i < buffer.Length; i++)
		{
			hash = hash * 31 + buffer[i];
		}
		return hash;
	}

	readonly int stringSizeLimit;
	int[] buckets; // contains index into entries offset by -1. So that 0 (default) means empty bucket.
	Entry[] entries;
	string[] lastStrings;

	int count;

	/// <summary>
	/// Creates a new StringPool instance.
	/// </summary>
	public CachingStringPool() : this(DefaultSizeLimit) { }

	/// <summary>
	/// Creates a new StringPool instance.
	/// </summary>
	/// <param name="stringSizeLimit">The size limit beyond which strings will not be pooled.</param>
	/// <remarks>
	/// The <paramref name="stringSizeLimit"/> prevents pooling strings beyond a certain size. 
	/// Longer strings are typically less likely to be duplicated, and and carry extra cost for identifying uniqueness.
	/// </remarks>
	public CachingStringPool(int stringSizeLimit)
	{
		int size = GetSize(DefaultCapacity);
		this.stringSizeLimit = stringSizeLimit;
		this.buckets = new int[size];
		this.entries = new Entry[size];
		this.lastStrings = new string[8];
		for (int i = 0; i < this.lastStrings.Length; i++)
		{
			this.lastStrings[i] = string.Empty;
		}
	}

	static int GetSize(int capacity)
	{
		var size = DefaultCapacity;
		while (size < capacity)
			size = size * 2;
		return size;
	}

	///// <summary>
	///// Gets a string containing the characters in the input buffer.
	///// </summary>
	//public string GetString(char[] buffer, int offset, int length)
	//{
	//	return GetString(buffer.AsSpan(offset, length));
	//}

	/// <summary>
	/// Gets a string containing the characters in the input buffer.
	/// </summary>
#pragma warning disable CA1801 // Review unused parameters
	public string GetString(DbDataReader reader, int ordinal, char[] buffer, int offset, int length)
#pragma warning restore CA1801 // Review unused parameters
	{
		return GetString(ordinal, buffer.AsSpan(offset, length));
	}

	/// <summary>
	/// Gets a string containing the characters in the input buffer.
	/// </summary>
	public string GetString(int ordinal, char[] buffer, int offset, int length)
	{
		return GetString(ordinal, buffer.AsSpan(offset, length));
	}

	///// <summary>
	///// Gets a string containing the characters in the input buffer.
	///// </summary>
	//public string GetString(ReadOnlySpan<char> buffer)
	//{
	//	return GetString(0, buffer);
	//}

	/// <summary>
	/// Gets a string for the given column containing the input buffer.
	/// </summary>
	public string GetString(int ordinal, ReadOnlySpan<char> buffer)
	{
		var length = buffer.Length;

		if (buffer == null) throw new ArgumentNullException(nameof(buffer));
		if (length == 0) return string.Empty;
		if (length > stringSizeLimit)
		{
			return new string(buffer);
		}

		var lastStrs = this.lastStrings;

		if (ordinal >= lastStrs.Length)
		{
			Array.Resize(ref this.lastStrings, ordinal + 8);
			lastStrs = this.lastStrings;
			for (int x = 0; x < lastStrs.Length; x++)
			{
				if (lastStrs[x] == null)
				{
					lastStrs[x] = string.Empty;
				}
			}
		}

		var ls = lastStrs[ordinal];
		if (MemoryExtensions.SequenceEqual(buffer, ls.AsSpan()))
		{
			return ls;
		}

		var entries = this.entries;
		var hashCode = GetHashCode(buffer);

		uint collisionCount = 0;
		ref int bucket = ref GetBucket(hashCode);
		int i = bucket - 1;

		while ((uint)i < (uint)entries.Length)
		{
			ref var e = ref entries[i];
			var str = e.str;
			if (e.hashCode == hashCode && MemoryExtensions.SequenceEqual(buffer, str.AsSpan()))
			{
				lastStrings[ordinal] = str;
				return str;
			}

			i = e.next;

			collisionCount++;
			if (collisionCount > CollisionLimit)
			{
				// protects against malicious inputs
				// too many collisions give up and let the caller create the string.					
				return new string(buffer);
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
		lastStrs[ordinal] = stringValue;

		ref Entry entry = ref entries![index];
		entry.hashCode = hashCode;
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
		public string str;
	}
}

#endif