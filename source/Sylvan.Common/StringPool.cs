using System;
using System.Runtime.CompilerServices;

namespace Sylvan;

/// <summary>
/// Implements a specialized hashtable to provide string de-duping capabilities.
/// </summary>
/// <remarks>
/// This class is useful for serializers where the data being deserialized might
/// contain highly repetetive string values and allows de-duping such strings while
/// avoiding allocations. 
/// This was directly intended to be used with Sylvan.Data.Csv.CsvDataReader.
/// </remarks>
public sealed class StringPool
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

	readonly int stringSizeLimit;
	int[] buckets; // contains index into entries offset by -1. So that 0 (default) means empty bucket.
	Entry[] entries;

	// When accessed with "ordinal", we keep track of the last string that was produced and as
	// a fast-path check if the same string is being returned again. This idea was taken from
	// github.com/nietras/sep library. It is an optimization that is very specific to the
	// benchmarks in github.com/joelverhagen/ncsvperf, but might be beneficial for real-world
	// datasets as well.
	string?[] lastStrings;

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
	/// Longer strings are typically less likely to be duplicated, and and carry extra cost for identifying uniqueness.
	/// </remarks>
	public StringPool(int stringSizeLimit)
	{
		int size = GetSize(DefaultCapacity);
		this.stringSizeLimit = stringSizeLimit;
		this.buckets = new int[size];
		this.entries = new Entry[size];
		this.lastStrings = Array.Empty<string>();
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
		return GetString(buffer.AsSpan(offset, length));
	}

	/// <summary>
	/// Gets a string containing the characters in the input buffer.
	/// </summary>
#pragma warning disable CA1801 // Review unused parameters
	// reader argument is to satisfy the signature required by ColumnStringFactory in the Sylvan.Data.Csv library.
	public string GetString(System.Data.Common.DbDataReader reader, int ordinal, char[] buffer, int offset, int length)
#pragma warning restore CA1801 // Review unused parameters
	{
		return GetString(ordinal, buffer.AsSpan(offset, length));
	}

	/// <summary>
	/// Gets a string containing the characters in the input buffer.
	/// </summary>
	public string GetString(ReadOnlySpan<char> buffer)
	{
		return GetString(-1, buffer);
	}

	static void FillEmpty(string?[] arr)
	{
		for (int i = 0; i < arr.Length; i++)
		{
			if (arr[i] == null)
				arr[i] = string.Empty;
		}
	}

	string GetString(int ordinal, ReadOnlySpan<char> buffer)
	{
		if (buffer == null) throw new ArgumentNullException(nameof(buffer));

		var length = buffer.Length;
		var str = string.Empty;
		if (length == 0) return str;
		if (length > stringSizeLimit)
		{
			return CreateString(buffer);
		}

		var lastStrs = this.lastStrings;

		if (ordinal >= 0)
		{
			if (ordinal >= lastStrs.Length)
			{
				Array.Resize(ref this.lastStrings, ordinal + 8);
				lastStrs = this.lastStrings;
				FillEmpty(lastStrs);
			}

			str = lastStrs[ordinal]!;
			if (MemoryExtensions.SequenceEqual(buffer, str.AsSpan()))
			{
				return str;
			}
		}

		var entries = this.entries;
		var hashCode = GetHashCode(buffer, 0, length);

		uint collisionCount = 0;
		ref int bucket = ref GetBucket(hashCode);
		int i = bucket - 1;

		while ((uint)i < (uint)entries.Length)
		{
			ref var e = ref entries[i];
			str = e.str;
			if (e.hashCode == hashCode && MemoryExtensions.SequenceEqual(buffer, str.AsSpan()))
			{
				if (ordinal >= 0)
				{
					lastStrs[ordinal] = str;
				}
				return str;
			}

			i = e.next;

			collisionCount++;
			if (collisionCount > CollisionLimit)
			{
				// protects against malicious inputs
				// too many collisions give up and let the caller create the string.					
				return CreateString(buffer);
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

		str = CreateString(buffer);

		if (ordinal >= 0)
		{
			lastStrs[ordinal] = str;
		}

		ref Entry entry = ref entries![index];
		entry.hashCode = hashCode;
		entry.next = bucket - 1;
		entry.str = str;

		bucket = index + 1; // bucket is an int ref

		return str;
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
	static string CreateString(ReadOnlySpan<char> chars)
	{
#if !NETSTANDARD2_0
		return new string(chars);
#else
		
		return GetStringUnsafe(chars);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		static unsafe string GetStringUnsafe(ReadOnlySpan<char> chars)
		{
			fixed (char* pValue = &chars.GetPinnableReference())
			{
				return new string(pValue, 0, chars.Length);
			}
		}
#endif
	}

	struct Entry
	{
		public uint hashCode;
		public int next;
		public string str;
	}
}
