using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Sylvan.Data.XBase
{
	partial class XBaseDataReader
	{
		abstract class DataAccessor
		{
			public virtual bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				return false;
			}

			/// <summary>
			/// Indicates if the dbase type can hold a null value.
			/// Date, for example can be filled with ' ', which has no meaningful value.
			/// Int32, on the other hand, is store in a 4-byte binary representation
			/// so cannot represent a null value.
			/// </summary>
			public virtual bool CanBeNull => false;

			public virtual DateTime GetDateTime(XBaseDataReader dr, int ordinal)
			{
				throw new NotSupportedException();
			}

			public virtual short GetInt16(XBaseDataReader dr, int ordinal)
			{
				return checked((short)GetInt64(dr, ordinal));
			}

			public virtual int GetInt32(XBaseDataReader dr, int ordinal)
			{
				return checked((int)GetInt64(dr, ordinal));
			}

			public virtual long GetInt64(XBaseDataReader dr, int ordinal)
			{
				var value = GetDecimal(dr, ordinal);

				if (value > long.MaxValue || value < long.MinValue)
				{
					throw new InvalidCastException();
				}

				var intVal = (long)value;
				if (intVal != value)
				{
					throw new InvalidCastException();
				}
				return intVal;
			}

			public virtual string GetString(XBaseDataReader dr, int ordinal)
			{
				return GetRawAsciiString(dr, ordinal);
			}

			public string GetRawAsciiString(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				return Encoding.ASCII.GetString(dr.recordBuffer, col.offset, col.length);
			}

#if NETSTANDARD2_1

			Span<byte> GetRecordSpan(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				return dr.recordBuffer.AsSpan().Slice(col.offset, col.length);
			}

			public virtual decimal GetDecimal(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				decimal value;
				int c;

				var span = dr.recordBuffer.AsSpan().Slice(col.offset, col.length);
				int i;
				for(i = 0; i < span.Length; i++)
				{
					if (span[i] != ' ')
						break;
				}
				span = span.Slice(i);

				if (System.Buffers.Text.Utf8Parser.TryParse(span, out value, out c))
				{
					return value;
				}
				else
				{
					if (System.Buffers.Text.Utf8Parser.TryParse(span, out double d, out c))
					{
						return (decimal)d;
					}
				}
				throw new FormatException();
			}
#else
			public virtual decimal GetDecimal(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				decimal value;
				if (TryParseAsciiDecimal(dr.recordBuffer, col.offset, col.length, out value))
				{
					return value;
				}
				else
				{
					if (TryParseDecimal(dr, ordinal, out value))
						return value;
				}

				throw new FormatException();
			}

			bool TryParseDecimal(XBaseDataReader dr, int ordinal, out decimal value)
			{

				var str = this.GetString(dr, ordinal);
				if (decimal.TryParse(str, out value))
				{
					return true;
				}
				else
				{
					// for scientific notation, which decimal does not support.
					if (double.TryParse(str, out double dValue))
					{
						// this cast can throw.
						value = (decimal)dValue;
						return true;
					}
				}
				return false;
			}

			// this is a non-allocating fast-path for the common case, attempts
			// to parse directly out of the byte[] instead of needing to convert to chars
			static bool TryParseAsciiDecimal(byte[] buffer, int offset, int length, out decimal value)
			{
				ulong val = 0;
				value = decimal.Zero;

				bool neg = false;
				int decIdx = -1;
				for (int i = 0; i < length; i++)
				{
					byte b = buffer[offset + i];
					switch (b)
					{
						case (byte)' ':
							break;
						case (byte)'-':
							if (neg)
							{
								return false;
							}
							neg = true;
							break;
						case (byte)'.':
							if (decIdx != -1)
								return false;
							decIdx = i;
							break;
						case (byte)'0':
						case (byte)'1':
						case (byte)'2':
						case (byte)'3':
						case (byte)'4':
						case (byte)'5':
						case (byte)'6':
						case (byte)'7':
						case (byte)'8':
						case (byte)'9':
							val = (val * 10) + (ulong)(b - '0');
							break;
						default:
							return false;
					}
				}
				var scale = decIdx == -1 ? 0 : length - decIdx - 1;

				unchecked
				{
					value = new Decimal((int)(val & 0xffffffff), (int)(val >> 32), 0, neg, (byte)scale);
				}
				return true;
			}

#endif

			public virtual float GetFloat(XBaseDataReader dr, int ordinal)
			{
				throw new NotSupportedException();
			}

			public virtual double GetDouble(XBaseDataReader dr, int ordinal)
			{
				var value = this.GetDecimal(dr, ordinal);
				return (double)value;
			}

			public virtual bool GetBoolean(XBaseDataReader dr, int ordinal)
			{
				throw new NotSupportedException();
			}

			// allow the raw binary representation to be accessed for all fields
			public virtual int GetBytes(XBaseDataReader dr, int ordinal, int dataOffset, byte[] buffer, int bufferOffset, int length)
			{
				return GetBytesRaw(dr, ordinal, dataOffset, buffer, bufferOffset, length);
			}

			public int GetBytesRaw(XBaseDataReader dr, int ordinal, int dataOffset, byte[] buffer, int bufferOffset, int length)
			{
				int off = dataOffset;
				var col = dr.columns[ordinal];
				var offset = col.offset;
				var len = col.length;

				var count = Math.Min(len - off, length);

				Buffer.BlockCopy(dr.recordBuffer, offset + off, buffer, bufferOffset, count);
				return count;
			}

			public virtual int GetChars(XBaseDataReader dr, int ordinal, int dataOffset, char[] buffer, int offset, int length)
			{
				throw new NotSupportedException();
			}

			public virtual Stream GetStream(XBaseDataReader dr, int ordinal)
			{
				throw new NotSupportedException();
			}

			public virtual TextReader GetTextReader(XBaseDataReader dr, int ordinal)
			{
				return new StreamReader(this.GetStream(dr, ordinal), dr.encoding);
			}

			public virtual Guid GetGuid(XBaseDataReader dr, int ordinal)
			{
#if NETSTANDARD2_1
				var span = GetRecordSpan(dr, ordinal);
				if(System.Buffers.Text.Utf8Parser.TryParse(span, out Guid value, out _))
				{
					return value;
				}
#else
				var str = GetRawAsciiString(dr, ordinal);
				return Guid.Parse(str);

#endif
				throw new FormatException();
			}
		}

		sealed class UnknownAccessor : DataAccessor
		{
			public static readonly UnknownAccessor Instance = new UnknownAccessor();

			public override bool CanBeNull => true;

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				return false;
			}
		}

		sealed class FoxProNullAccessor : DataAccessor
		{
			DataAccessor inner;

			public FoxProNullAccessor(DataAccessor inner)
			{
				this.inner = inner;
			}

			public override bool CanBeNull => true;

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				return
					NullFlagsAccessor.Instance.GetBoolean(dr, ordinal) ||
					inner.IsDBNull(dr, ordinal);
			}

			public override bool GetBoolean(XBaseDataReader dr, int ordinal)
			{
				return inner.GetBoolean(dr, ordinal);
			}

			public override int GetBytes(XBaseDataReader dr, int ordinal, int dataOffset, byte[] buffer, int offset, int length)
			{
				return inner.GetBytes(dr, ordinal, dataOffset, buffer, offset, length);
			}

			public override DateTime GetDateTime(XBaseDataReader dr, int ordinal)
			{
				return inner.GetDateTime(dr, ordinal);
			}

			public override decimal GetDecimal(XBaseDataReader dr, int ordinal)
			{
				return inner.GetDecimal(dr, ordinal);
			}

			public override double GetDouble(XBaseDataReader dr, int ordinal)
			{
				return inner.GetDouble(dr, ordinal);
			}

			public override int GetInt32(XBaseDataReader dr, int ordinal)
			{
				return inner.GetInt32(dr, ordinal);
			}

			public override string GetString(XBaseDataReader dr, int ordinal)
			{
				return inner.GetString(dr, ordinal);
			}

			public override int GetChars(XBaseDataReader dr, int ordinal, int dataOffset, char[] buffer, int offset, int length)
			{
				return inner.GetChars(dr, ordinal, dataOffset, buffer, offset, length);
			}

			public override Guid GetGuid(XBaseDataReader dr, int ordinal)
			{
				return inner.GetGuid(dr, ordinal);
			}

			public override Stream GetStream(XBaseDataReader dr, int ordinal)
			{
				return inner.GetStream(dr, ordinal);
			}

			public override TextReader GetTextReader(XBaseDataReader dr, int ordinal)
			{
				return inner.GetTextReader(dr, ordinal);
			}
		}

		sealed class NullFlagsAccessor : DataAccessor
		{
			public static readonly NullFlagsAccessor Instance = new NullFlagsAccessor();

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				return GetBoolean(dr, ordinal);
			}

			public override bool GetBoolean(XBaseDataReader dr, int ordinal)
			{
				// fox pro null flags column
				var flagIdx = dr.columns[ordinal].nullFlagIdx;
				if (flagIdx < 0) return false;

				return GetFlag(dr.recordBuffer, dr.nullFlagsColumn!.offset, flagIdx);
			}

			internal static bool GetFlag(byte[] buffer, int offset, int flagIdx)
			{
				var i = flagIdx >> 3;
				var b = buffer[offset + i];
				var bit = 1 << (flagIdx & 0x7);
				bool flag = (b & bit) != 0;
				return flag;
			}
		}

		sealed class DateAccessor : DataAccessor
		{
			public static readonly DateAccessor Instance = new DateAccessor();

			public override bool CanBeNull => true;

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				var b = dr.recordBuffer;
				var o = col.offset;
				return b[o] == ' ';
			}

			public override DateTime GetDateTime(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				var o = col.offset;
				var b = dr.recordBuffer;

				if (b[o] == ' ')
					throw new InvalidCastException();

				// TODO: this could probably use some range validation.

				var y =
					(b[o + 0] - '0') * 1000 +
					(b[o + 1] - '0') * 100 +
					(b[o + 2] - '0') * 10 +
					(b[o + 3] - '0');
				var m =
					(b[o + 4] - '0') * 10 +
					(b[o + 5] - '0');
				var d =
					(b[o + 6] - '0') * 10 +
					(b[o + 7] - '0');

				return new DateTime(y, m, d, 0, 0, 0, DateTimeKind.Unspecified);
			}
		}

		sealed class DateTimeAccessor : DataAccessor
		{
			public static readonly DateTimeAccessor Instance = new DateTimeAccessor();

			public override DateTime GetDateTime(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				var o = col.offset;
				var b = dr.recordBuffer;
				var date = BitConverter.ToInt32(b, o);
				var time = BitConverter.ToInt32(b, o + 4);
				// don't ask me if this magic number has any meaning
				var value = DateTime.MinValue.AddDays(date - 1721426).AddMilliseconds(time);
				return value;
			}
		}

		sealed class Int32Accessor : DataAccessor
		{
			public static readonly Int32Accessor Instance = new Int32Accessor();

			public override int GetInt32(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				return BitConverter.ToInt32(dr.recordBuffer, col.offset);
			}
		}

		sealed class CharacterAccessor : DataAccessor
		{
			public static readonly CharacterAccessor Instance = new CharacterAccessor();

			public override string GetString(XBaseDataReader dr, int ordinal)
			{
				//var str = dr.GetRecordChars(ordinal);
				var col = dr.columns[ordinal];
				var buf = dr.textBuffer;

				int count = dr.encoding.GetChars(dr.recordBuffer, col.offset, col.length, buf, 0);

				int i = count - 1;
				for (; i >= 0; i--)
				{
					if (buf[i] != ' ')
						break;
				}

				return i <= 0 ? string.Empty : new string(buf, 0, i);
			}
		}

		sealed class VarCharAccessor : DataAccessor
		{
			public static readonly VarCharAccessor Instance = new VarCharAccessor();

			public override string GetString(XBaseDataReader dr, int ordinal)
			{
				var buf = dr.recordBuffer;

				var col = dr.columns[ordinal];
				var varFlagIdx = col.varFlagIdx;
				Debug.Assert(varFlagIdx >= 0);
				var hasVar = NullFlagsAccessor.GetFlag(buf, dr.nullFlagsColumn!.offset, varFlagIdx);
				var length =
					hasVar
					? buf[col.offset + col.length - 1]
					: col.length;

				var str = dr.encoding.GetString(buf, col.offset, length);
				return str;
			}
		}

		sealed class VarBinaryAccessor : DataAccessor
		{
			public static readonly VarBinaryAccessor Instance = new VarBinaryAccessor();

			public override int GetBytes(XBaseDataReader dr, int ordinal, int dataOffset, byte[] buffer, int bufferOffset, int length)
			{
				var buf = dr.recordBuffer;

				var col = dr.columns[ordinal];
				var varFlagIdx = col.varFlagIdx;
				Debug.Assert(varFlagIdx >= 0);
				var hasVar = NullFlagsAccessor.GetFlag(buf, dr.nullFlagsColumn!.offset, varFlagIdx);
				var varLen =
					hasVar
					? buf[col.offset + col.length - 1]
					: col.length;

				var count = Math.Min(varLen - dataOffset, length);
				Buffer.BlockCopy(dr.recordBuffer, col.offset + dataOffset, buffer, bufferOffset, count);
				return count;
			}
		}

		sealed class MemoAccessor : DataAccessor
		{
			public static readonly MemoAccessor Instance = new MemoAccessor();

			public override bool CanBeNull => true;

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				var idx = BitConverter.ToInt32(dr.recordBuffer, col.offset);
				return idx == 0;
			}

			public override int GetChars(XBaseDataReader dr, int ordinal, int dataOffset, char[] buffer, int offset, int length)
			{
				string str =
					dr.cachedRecordRow == dr.recordIdx && dr.cachedRecordOrdinal == ordinal
					? dr.cachedRecord
					: this.GetString(dr, ordinal);

				var c = Math.Min(str.Length - dataOffset, length);
				str.CopyTo(dataOffset, buffer, offset, c);
				return c;
			}

			public override TextReader GetTextReader(XBaseDataReader dr, int ordinal)
			{
				return new StreamReader(this.GetStream(dr, ordinal), dr.encoding, false);
			}

			public override Stream GetStream(XBaseDataReader dr, int ordinal)
			{
				return new MemoStream(dr, ordinal);
			}

			public override string GetString(XBaseDataReader dr, int ordinal)
			{
				using var r = this.GetTextReader(dr, ordinal);
				var str = r.ReadToEnd();
				dr.cachedRecord = str;
				dr.cachedRecordOrdinal = ordinal;
				dr.cachedRecordRow = dr.recordIdx;
				return str;
			}
		}

		sealed class BlobAccessor : DataAccessor
		{
			public static readonly BlobAccessor Instance = new BlobAccessor();

			public override bool CanBeNull => true;

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				var idx = BitConverter.ToInt32(dr.recordBuffer, col.offset);
				return idx == 0;
			}

			public override int GetBytes(XBaseDataReader dr, int ordinal, int dataOffset, byte[] buffer, int bufferOffset, int length)
			{
				// todo: this is horribly inefficient.
				var s = GetStream(dr, ordinal);
				s.Seek(dataOffset, SeekOrigin.Begin);
				return s.Read(buffer, bufferOffset, length);
			}

			public override Stream GetStream(XBaseDataReader dr, int ordinal)
			{
				return new MemoStream(dr, ordinal);
			}
		}

		sealed class NullAccessor : DataAccessor
		{
			public static readonly NullAccessor Instance = new NullAccessor();

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				return true;
			}
		}

		sealed class BooleanAccessor : DataAccessor
		{
			public static readonly BooleanAccessor Instance = new BooleanAccessor();

			public override bool CanBeNull => true;

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				return dr.recordBuffer[col.offset] == ' ';
			}

			public override bool GetBoolean(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				byte value = dr.recordBuffer[col.offset];
				return value == 'T' || value == 't';
			}
		}

		sealed class CurrencyAccessor : DataAccessor
		{
			public static readonly CurrencyAccessor Instance = new CurrencyAccessor();

			public override decimal GetDecimal(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				var dec = (decimal)BitConverter.ToInt64(dr.recordBuffer, col.offset);
				dec = dec / 10000m;
				return dec;
			}
		}

		sealed class NumericAccessor : DataAccessor
		{
			public static readonly NumericAccessor Instance = new NumericAccessor();

			public override bool CanBeNull => true;

			public override bool IsDBNull(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				return dr.recordBuffer[col.offset + col.length - 1] == ' ';
			}

			// uses the base implementation of GetDecimal
		}

		sealed class DoubleAccessor : DataAccessor
		{
			public static readonly DoubleAccessor Instance = new DoubleAccessor();

			public override double GetDouble(XBaseDataReader dr, int ordinal)
			{
				var col = dr.columns[ordinal];
				return BitConverter.ToDouble(dr.recordBuffer, col.offset);
			}
		}

		static Dictionary<ushort, ushort> CodePageMap;

		static XBaseDataReader()
		{
			CodePageMap = new Dictionary<ushort, ushort>();

			// maps the xBase header encoding number to the codePage number
			ushort[] codePageData = new ushort[]
			{
				 0x01, 437 ,
				 0x69, 620 , // *
				 0x6a, 737 ,
				 0x02, 850 ,
				 0x64, 852 ,
				 0x6b, 857 ,
				 0x67, 861 ,
				 0x66, 865 ,
				 0x65, 866 ,
				 0x7c, 874 ,
				 0x68, 895 , // *
				 0x7b, 932 ,
				 0x7a, 936 ,
				 0x79, 949 ,
				 0x78, 950 ,
				 0xc8, 1250 ,
				 0xc9, 1251 ,
				 0x03, 1252 ,
				 0xcb, 1253 ,
				 0xca, 1254 ,
				 0x7d, 1255 ,
				 0x7e, 1256 ,
				 0x7f, 65001, // TODO: Not sure about this one.
				 0x04, 10000 ,
				 0x98, 10006 ,
				 0x96, 10007 ,
				 0x97, 10029 ,

			};
			// *: Not supported by the CodePagesEncodingProvider, unlikely that anyone would care.

			for (int i = 0; i < codePageData.Length; i += 2)
			{
				CodePageMap.Add(codePageData[i], codePageData[i + 1]);
			}
		}
	}
}
