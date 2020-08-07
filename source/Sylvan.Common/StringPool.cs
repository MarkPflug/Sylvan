using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sylvan
{
	public interface IStringPool
	{
		string GetString(ReadOnlySpan<char> str);
	}

	sealed partial class StringPool : IStringPool
	{
		static uint GetHashCode(ReadOnlySpan<char> str)
		{
			uint hash = 0;
			for (int i = 0; i < str.Length; i++)
			{
				hash = hash * 31 + str[i];
			}
			return hash;
		}

		int[] buckets;
		Entry[] entries;

		int count;
		int freeList;
		int freeCount;

		const int StartOfFreeList = -3;

		public StringPool(int capacity)
		{
			int size = GetPrime(capacity);
			this.buckets = new int[size];
			this.entries = new Entry[size];

			// Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
			freeList = -1;
#if TARGET_64BIT
            fastModMultiplier = HashHelpers.GetFastModMultiplier((uint)size);
#endif
		}

		public string GetString(ReadOnlySpan<char> str)
		{
			var entries = this.entries;

			var hashCode = GetHashCode(str);

			uint collisionCount = 0;
			ref int bucket = ref GetBucket(hashCode);
			int i = bucket - 1; // Value in _buckets is 1-based

			while ((uint)i < (uint)entries.Length)
			{
				ref var e = ref entries[i];
				if (e.hashCode == hashCode && MemoryExtensions.Equals(str, e.str.AsSpan(), StringComparison.Ordinal))
				{
					return e.str;
				}

				i = e.next;

				collisionCount++;
				if (collisionCount > (uint)entries.Length)
				{
					throw new InvalidOperationException();
				}
			}

			int index;
			if (this.freeCount > 0)
			{
				index = this.freeList;
				//Debug.Assert((StartOfFreeList - entries[_freeList].next) >= -1, "shouldn't overflow because `next` cannot underflow");
				this.freeList = StartOfFreeList - entries[this.freeList].next;
				this.freeCount--;
			}
			else
			{
				int count = this.count;
				if (count == entries.Length)
				{
					Resize();
					bucket = ref GetBucket(hashCode);
				}
				index = count;
				this.count = count + 1;
				entries = this.entries;
			}

			var stringValue = str.ToString();
			ref Entry entry = ref entries![index];
			entry.hashCode = hashCode;
			entry.next = bucket - 1; // Value in _buckets is 1-based
			entry.str = stringValue;
			bucket = index + 1;		

			return stringValue;
		}

		void Resize()
		{
			Resize(ExpandPrime(this.count));
		}

		void Resize(int newSize)
		{
			Debug.Assert(newSize >= this.entries.Length);

			var entries = new Entry[newSize];

			int count = this.count;
			Array.Copy(this.entries, entries, count);

			// Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
			this.buckets = new int[newSize];
#if TARGET_64BIT
            _fastModMultiplier = GetFastModMultiplier((uint)newSize);
#endif
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
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private ref int GetBucket(uint hashCode)
		{
			int[] buckets = this.buckets!;
#if TARGET_64BIT
            return ref buckets[FastMod(hashCode, (uint)buckets.Length, _fastModMultiplier)];
#else
			return ref buckets[hashCode % (uint)buckets.Length];
#endif
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

		public const uint HashCollisionThreshold = 100;

		// This is the maximum prime smaller than Array.MaxArrayLength
		public const int MaxPrimeArrayLength = 0x7FEFFFFD;

		public const int HashPrime = 101;

		// Table of prime numbers to use as hash table sizes.
		// A typical resize algorithm would pick the smallest prime number in this array
		// that is larger than twice the previous capacity.
		// Suppose our Hashtable currently has capacity x and enough elements are added
		// such that a resize needs to occur. Resizing first computes 2x then finds the
		// first prime in the table greater than 2x, i.e. if primes are ordered
		// p_1, p_2, ..., p_i, ..., it finds p_n such that p_n-1 < 2x < p_n.
		// Doubling is important for preserving the asymptotic complexity of the
		// hashtable operations such as add.  Having a prime guarantees that double
		// hashing does not lead to infinite loops.  IE, your hash function will be
		// h1(key) + i*h2(key), 0 <= i < size.  h2 and the size must be relatively prime.
		// We prefer the low computation costs of higher prime numbers over the increased
		// memory allocation of a fixed prime number i.e. when right sizing a HashSet.
		private static readonly int[] s_primes =
		{
			3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
			1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
			17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
			187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
			1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
		};

		public static bool IsPrime(int candidate)
		{
			if ((candidate & 1) != 0)
			{
				int limit = (int)Math.Sqrt(candidate);
				for (int divisor = 3; divisor <= limit; divisor += 2)
				{
					if ((candidate % divisor) == 0)
						return false;
				}
				return true;
			}
			return candidate == 2;
		}

		public static int GetPrime(int min)
		{
			if (min < 0) throw new ArgumentOutOfRangeException(nameof(min));

			foreach (int prime in s_primes)
			{
				if (prime >= min)
					return prime;
			}

			// Outside of our predefined table. Compute the hard way.
			for (int i = (min | 1); i < int.MaxValue; i += 2)
			{
				if (IsPrime(i) && ((i - 1) % HashPrime != 0))
					return i;
			}
			return min;
		}

		// Returns size of hashtable to grow to.
		public static int ExpandPrime(int oldSize)
		{
			int newSize = 2 * oldSize;

			// Allow the hashtables to grow to maximum possible size (~2G elements) before encountering capacity overflow.
			// Note that this check works even when _items.Length overflowed thanks to the (uint) cast
			if ((uint)newSize > MaxPrimeArrayLength && MaxPrimeArrayLength > oldSize)
			{
				Debug.Assert(MaxPrimeArrayLength == GetPrime(MaxPrimeArrayLength), "Invalid MaxPrimeArrayLength");
				return MaxPrimeArrayLength;
			}

			return GetPrime(newSize);
		}

		/// <summary>Returns approximate reciprocal of the divisor: ceil(2**64 / divisor).</summary>
		/// <remarks>This should only be used on 64-bit.</remarks>
		public static ulong GetFastModMultiplier(uint divisor) =>
			ulong.MaxValue / divisor + 1;

		/// <summary>Performs a mod operation using the multiplier pre-computed with <see cref="GetFastModMultiplier"/>.</summary>
		/// <remarks>This should only be used on 64-bit.</remarks>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static uint FastMod(uint value, uint divisor, ulong multiplier)
		{
			// We use modified Daniel Lemire's fastmod algorithm (https://github.com/dotnet/runtime/pull/406),
			// which allows to avoid the long multiplication if the divisor is less than 2**31.
			Debug.Assert(divisor <= int.MaxValue);

			// This is equivalent of (uint)Math.BigMul(multiplier * value, divisor, out _). This version
			// is faster than BigMul currently because we only need the high bits.
			uint highbits = (uint)(((((multiplier * value) >> 32) + 1) * divisor) >> 32);

			Debug.Assert(highbits == value % divisor);
			return highbits;
		}
	}
}
