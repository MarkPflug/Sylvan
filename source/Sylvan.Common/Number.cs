using System;
using System.Globalization;
using System.Runtime.CompilerServices;

#if NETSTANDARD2_1
using System.Buffers;
#endif

namespace Sylvan
{
	public static partial class Number
	{
		const int PoolSize = 256;
		const int PoolMask = ~0xff;

		static string[] IntStrings = new string[PoolSize];

		public static int ParseInt(ReadOnlySpan<char> text)
		{
			int val;
			return
				TryParseInt(text, out val)
				? val
				: throw new FormatException();
		}

		public static bool TryParseInt(ReadOnlySpan<char> text, out int value)
		{
			value = 0;
			int accum = 0;
			if (text.Length == 0) return false;

			int i = 0;

			bool neg = false;
			if (text[i] == '-')
			{
				neg = true;
				i = 1;
			}

			for (; i < text.Length; i++)
			{
				var c = text[i];
				if (c < '0' || c > '9')
				{
					return false;
				}
				accum = accum * 10 + (text[i] - '0');
			}

			value = neg ? -accum : accum;
			return true;
		}

		public static uint ParseUInt(ReadOnlySpan<char> text)
		{
			uint val;
			return
				TryParseUInt(text, out val)
				? val
				: throw new FormatException();
		}

		public static bool TryParseUInt(ReadOnlySpan<char> text, out uint value)
		{
			value = 0;
			uint accum = 0;
			if (text.Length == 0) return false;

			int i = 0;

			for (; i < text.Length; i++)
			{
				var c = text[i];
				if (c < '0' || c > '9')
				{
					return false;
				}
				accum = accum * 10u + (uint)(text[i] - '0');
			}

			value = accum;
			return true;
		}

		/// <summary>
		/// Converts the integer to a string. 
		/// Integers in the range between 0 and 255 will be shared and interned.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static string ToStringCommon(this int val)
		{
			return
				(val & PoolMask) == 0
				? IntStrings[val] ?? Interned((byte)val)
				: ToString(val);
		}

#if NETSTANDARD2_1

		static string Interned(byte val)
		{
			var len = StringLength(val);
			var str = string.Create(len, val, (span, v) => FormatByte(span, v)); 
			return IntStrings[val] = string.Intern(str);
		}
#else
		static string Interned(byte val)
		{
			var len = StringLength(val);
			var str = val.ToString(CultureInfo.InvariantCulture);
			return IntStrings[val] = string.Intern(str);
		}

#endif

		static string ToString(int val)
		{
			return val.ToString(CultureInfo.InvariantCulture);
		}

		public static int WriteInteger(this Memory<char> buffer, long value)
		{
			return WriteInteger(buffer.Span, value);
		}

		public static int WriteInteger(this Span<char> buffer, long value)
		{
			var neg = value < 0;
			return WriteInteger(buffer, neg ? (ulong)-value : (ulong)value, neg);
		}

		public static int WriteInteger(this Span<char> buffer, ulong value, bool negative)
		{
			int len = StringLength(value);

			if (negative)
			{
				len++;
				buffer[0] = '-';
			}

			int index = len;

			do
			{
				var div = value / 10;
				var digit = value - (10 * div);

				buffer[--index] = (char)('0' + digit);
				value = div;
			} while (value != 0);

			return len;
		}

		public static int WriteInteger(this Span<char> buffer, int value)
		{
			var neg = value < 0;
			return WriteInteger(buffer, neg ? (uint)-value : (uint)value, neg);
		}

		public static int WriteInteger(this Span<char> buffer, uint value, bool negative)
		{
			int len = StringLength(value);

			if (negative)
			{
				len++;
				buffer[0] = '-';
			}

			int index = len;

			do
			{
				uint div = value / 10;
				uint digit = value - (10 * div);

				buffer[--index] = (char)('0' + digit);
				value = div;
			} while (value != 0);

			return len;
		}

		public static int WriteByte(this Span<char> buffer, byte value)
		{
			var str = ToStringCommon(value);
			str.AsSpan().CopyTo(buffer);
			return str.Length;
		}

		static void FormatByte2(this Span<char> buffer, byte value)
		{
			FormatByte(buffer, value);
		}

		static int FormatByte(this Span<char> buffer, byte value)
		{
			int len = StringLength(value);
			int index = len;

			do
			{
				byte div = (byte)(value / 10);
				byte digit = (byte)(value - (10 * div));

				buffer[--index] = (char)('0' + digit);
				value = div;
			} while (value != 0);

			return len;
		}

		static int StringLength(long value)
		{
			var neg = value < 0;
			ulong val = neg ? (ulong)-value : (ulong)value;
			return StringLength(val) + (neg ? 1 : 0);
		}

		static int StringLength(int value)
		{
			var neg = value < 0;
			uint val = neg ? (uint)-value : (uint)value;
			return StringLength(val) + (neg ? 1 : 0);
		}


		[MethodImpl(MethodImplOptions.NoInlining)]
		static int StringLengthLong(ulong value)
		{
			if (value < 1_0000_0000_0000)
			{
				if (value < 1_0000_0000)
				{
					if (value < 1_00_0000)
					{
						if (value < 1_0_0000)
						{
							return 5;
						}
						else
						{
							return 6;
						}
					}
					else
					{
						if (value < 1_000_0000)
						{
							return 7;
						}
						else
						{
							return 8;
						}
					}
				}
				else
				{
					if (value < 1_00_0000_0000)
					{
						if (value < 1_0_0000_0000)
						{
							return 9;
						}
						else
						{
							return 10;
						}
					}
					else
					{
						if (value < 1_000_0000_0000)
						{
							return 11;
						}
						else
						{
							return 12;
						}
					}
				}
			}
			else
			{
				if (value < 1_0000_0000_0000_0000)
				{
					if (value < 1_00_0000_0000_0000)
					{
						if (value < 1_0_0000_0000_0000)
						{
							return 13;
						}
						else
						{
							return 14;
						}
					}
					else
					{
						if (value < 1_000_0000_0000_0000)
						{
							return 15;
						}
						else
						{
							return 16;
						}
					}
				}
				else
				{
					if (value < 1_00_0000_0000_0000_0000)
					{
						if (value < 1_0_0000_0000_0000_0000)
						{
							return 17;
						}
						else
						{
							return 18;
						}
					}
					else
					{
						if (value < 1_000_0000_0000_0000_0000)
						{
							return 19;
						}
						else
						{
							return 20;
						}
					}
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static int StringLength(ulong value)
		{
			// hard coded binary tree, balanced such that
			// values < 10000 require 3 comparisons,
			// while everything else requires 5
			if (value < 10000)
			{
				if (value < 100)
				{
					if (value < 10)
						return 1;
					else
						return 2;
				}
				else
				{
					if (value < 1000)
						return 3;
					else
						return 4;
				}
			}
			else
			{
				// long branch not inlined
				return StringLengthLong(value);
			}
		}

		internal static int StringLength(byte value)
		{
			if (value < 10) return 1;
			if (value < 100) return 2;
			return 3;
		}
	}
}
