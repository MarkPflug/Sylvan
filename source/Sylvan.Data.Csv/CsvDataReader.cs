using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// A data reader for delimited text data.
	/// </summary>
	public sealed partial class CsvDataReader : DbDataReader, IDbColumnSchemaGenerator
	{
		static readonly char[] AutoDetectDelimiters = new[] { ',', '\t', ';', '|' };

		struct Enumerator : IEnumerator
		{
			readonly CsvDataReader reader;

			public Enumerator(CsvDataReader reader)
			{
				this.reader = reader;
			}

			public object Current => reader;

			public bool MoveNext()
			{
				return reader.Read();
			}

			public void Reset()
			{
				throw new NotSupportedException();
			}
		}

		enum QuoteState
		{
			Unquoted = 0,
			Quoted = 1,
			BrokenQuotes = 2,
			ImplicitQuotes = 3,
		}

		struct FieldInfo
		{
			internal int endIdx;
			internal int escapeCount;
			internal QuoteState quoteState;

#if DEBUG
			public override string ToString()
			{
				return $"EndIdx: {endIdx} Quotes: {quoteState} EscapeCount: {escapeCount}";
			}
#endif
		}

		enum State
		{
			None = 0,
			Initializing,
			Initialized,
			Open,
			End,
			Closed,
		}

		enum ReadResult
		{
			False,
			True,
			Incomplete,
		}

		readonly TextReader reader;
		bool hasRows;
		readonly char[] buffer;
		int result;
		int idx;
		int bufferEnd;
		int recordStart;
		bool atEndOfText; // indicates if the buffer contains the last chunk of data.
		State state;
		int fieldCount; // fields in the header (or firstRow)
		int curFieldCount; // fields in current row
		int rowNumber;
		byte[]? scratch;
		FieldInfo[] fieldInfos;
		CsvColumn[] columns;
		bool autoDetectDelimiter;

		readonly Dictionary<string, int> headerMap;
		// options:
		char delimiter;
		readonly char quote;
		readonly char escape;
		readonly bool ownsReader;
		readonly bool implicitQuotes;
		readonly CultureInfo culture;
		readonly string? dateFormat;
		readonly string? trueString, falseString;
		readonly BinaryEncoding binaryEncoding;
		readonly bool hasHeaders;
		readonly StringFactory stringFactory;
		readonly ICsvSchemaProvider? schema;

		private CsvDataReader(TextReader reader, CsvDataReaderOptions? options = null)
		{
			if (options != null)
				options.Validate();
			options ??= CsvDataReaderOptions.Default;
			this.reader = reader;
			this.buffer = options.Buffer ?? new char[options.BufferSize];

			this.hasHeaders = options.HasHeaders;
			this.autoDetectDelimiter = options.Delimiter == null;
			this.delimiter = options.Delimiter ?? '\0';
			this.implicitQuotes = options.CsvStyle == CsvStyle.Unquoted;
			this.quote = options.Quote;
			this.escape = options.Escape;
			this.dateFormat = options.DateFormat;
			this.trueString = options.TrueString;
			this.falseString = options.FalseString;
			this.result = -1;
			this.recordStart = 0;
			this.bufferEnd = 0;
			this.idx = 0;
			this.rowNumber = 0;
			this.fieldInfos = new FieldInfo[16];
			this.headerMap = new Dictionary<string, int>(options.HeaderComparer);
			this.columns = Array.Empty<CsvColumn>();
			this.culture = options.Culture;
			this.ownsReader = options.OwnsReader;
			this.binaryEncoding = options.BinaryEncoding;
			this.stringFactory = options.StringFactory ?? new StringFactory((char[] b, int o, int l) => new string(b, o, l));
			this.schema = options.Schema;
		}
		

		char DetectDelimiter()
		{
			int[] counts = new int[AutoDetectDelimiters.Length];
			for (int i = 0; i < bufferEnd; i++)
			{
				var c = buffer[i];
				if (c == '\n' || c == '\r') {
					var x = counts.Sum();
					if(x == 0)
					{
						continue;
					}
					break;
				}
				for (int d = 0; d < AutoDetectDelimiters.Length; d++)
				{
					if (c == AutoDetectDelimiters[d])
					{
						counts[d]++;
					}
				}
			}
			int maxIdx = 0;
			int maxCount = 0;
			for (int i = 0; i < counts.Length; i++)
			{
				if (counts[i] > maxCount)
				{
					maxCount = counts[i];
					maxIdx = i;
				}
			}
			return AutoDetectDelimiters[maxIdx];
		}

		void InitializeSchema()
		{
			columns = new CsvColumn[this.fieldCount];
			for (int i = 0; i < this.fieldCount; i++)
			{
				var name = hasHeaders ? GetString(i) : null;
				var columnSchema = schema?.GetColumn(name, i);
				columns[i] = new CsvColumn(name, i, columnSchema);

				name = columns[i].ColumnName;
				if (!string.IsNullOrEmpty(name))
				{
					if (headerMap.ContainsKey(name))
					{
						// if we encounter duplicate headers track that
						// it is ambiguous. Attempts to access this by name
						// will result in an exception.
						headerMap[name] = -1;
					}
					else
					{
						headerMap.Add(name, i);
					}
				}
			}
			this.state = State.Initialized;
		}

		// attempt to read a field. 
		// returns True if there are more in record (hit delimiter), 
		// False if last in record (hit eol/eof), 
		// or Incomplete if we exhausted the buffer before finding the end of the record.
		ReadResult ReadField(int fieldIdx)
		{
			char c;
			var idx = this.idx;
			// this will remain -1 if it is unquoted. 
			// Otherwise we use it to determine if the quotes were "clean".
			var closeQuoteIdx = -1;
			int escapeCount = 0;
			int fieldEnd = 0;
			var buffer = this.buffer;
			bool last = false;
			bool complete = false;

			if (implicitQuotes)
			{
				// consume quoted field.
				while (idx < bufferEnd)
				{
					c = buffer[idx++];
					if (c == escape)
					{
						if (idx < bufferEnd)
						{
							c = buffer[idx++]; // the escaped char
							if (IsEndOfLine(c))
							{
								// if the escape preceed an EOL, we might have to consume 2 chars
								var r = ConsumeLineEnd(buffer, ref idx);
								if (r == ReadResult.Incomplete)
								{
									return ReadResult.Incomplete;
								}
							}
							escapeCount++;
							continue;
						}
						else
						{
							if (atEndOfText)
							{
								// TODO: not sure what to do here.
								escapeCount++;
								break;
							}
							return ReadResult.Incomplete;
						}
					}
					if (c == delimiter || IsEndOfLine(c))
					{
						// HACK: "unread" the delimiter/eol, and let the normal code path handle it
						idx--;
						break;
					}
				}
			}
			else
			{
				if (idx < bufferEnd)
				{
					c = buffer[idx];
					if (c == quote)
					{
						idx++;
						closeQuoteIdx = idx;

						// consume quoted field.
						while (idx < bufferEnd)
						{
							c = buffer[idx++];
							if (c == escape)
							{
								if (idx < bufferEnd)
								{
									c = buffer[idx++]; // the escaped char
									if (c == escape || c == quote)
									{
										escapeCount++;
										continue;
									}
									else
									if (escape == quote)
									{
										idx--;
										closeQuoteIdx = idx;
										// the quote (escape) we just saw was a the closing quote
										break;
									}
								}
								else
								{
									if (atEndOfText)
									{
										break;
									}
									return ReadResult.Incomplete;
								}
							}

							if (c == quote)
							{
								// immediately after the quote should be a delimiter, eol, or eof, but...
								// we can simply treat the remainder of the record like a normal unquoted field
								// we are currently positioned on the quote, the next while loop will consume it
								closeQuoteIdx = idx;
								break;
							}
							if (IsEndOfLine(c))
							{
								idx--;
								var r = ConsumeLineEnd(buffer, ref idx);
								if (r == ReadResult.Incomplete)
								{
									return ReadResult.Incomplete;
								}
								else
								{
									// continue on. We are inside a quoted string, so the newline is part of the value.
								}
							}
						} // we exit this loop when we reach the closing quote.
					}
				}
			}

			while (idx < bufferEnd)
			{
				c = buffer[idx++];

				if (c == delimiter)
				{
					this.idx = idx;
					fieldEnd = idx - 1 - recordStart;
					complete = true;
					break;
				}
				if (IsEndOfLine(c))
				{
					idx--;
					var temp = idx;
					var r = ConsumeLineEnd(buffer, ref idx);
					if (r == ReadResult.Incomplete)
					{
						return ReadResult.Incomplete;
					}
					fieldEnd = temp - recordStart;
					complete = true;
					last = true;
					break;
				}
			}

			if (complete || atEndOfText)
			{
				if (fieldIdx >= fieldInfos.Length)
				{
					// this resize is constrained by the fact that the record has to fit in one row
					Array.Resize(ref fieldInfos, fieldInfos.Length * 2);
				}
				
				curFieldCount++;
				
				ref var fi = ref fieldInfos[fieldIdx];

				if (implicitQuotes)
				{
					fi.quoteState =
						escapeCount == 0
						? QuoteState.Unquoted
						: QuoteState.ImplicitQuotes;
				}
				else
				{
					fi.quoteState =
						closeQuoteIdx == -1
						? QuoteState.Unquoted
						: fieldEnd == (closeQuoteIdx - recordStart)
						? QuoteState.Quoted
						: QuoteState.BrokenQuotes;
				}


				fi.escapeCount = escapeCount;
				fi.endIdx = complete ? fieldEnd : (idx - recordStart);
				
				this.idx = idx;

				if (complete)
					return last ? ReadResult.False : ReadResult.True;

				return ReadResult.False;
			}

			return ReadResult.Incomplete;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		static bool IsEndOfLine(char c)
		{
			return c == '\r' || c == '\n';
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		ReadResult ConsumeLineEnd(char[] buffer, ref int idx)
		{
			var c = buffer[idx++];
			if (c == '\r')
			{
				if (idx < bufferEnd)
				{
					c = buffer[idx++];
					if (c != '\n')
					{
						// weird, but we'll allow a lone \r
						idx--;
					}
					return ReadResult.True;
				}
				else
				{
					return atEndOfText
						? ReadResult.True
						// the next buffer might contain a \n that we need to consume.
						: ReadResult.Incomplete;
				}
			}
			if (c == '\n')
			{
				return ReadResult.True;
			}
			// this can never be reached.
			return ReadResult.False;
		}

		/// <inheritdoc/>
		public override object this[int ordinal] => this.GetValue(ordinal);

		/// <inheritdoc/>
		public override object this[string name] => this.GetValue(this.GetOrdinal(name));

		/// <inheritdoc/>
		public override int Depth => 0;

		/// <summary>
		/// Gets the number of fields in the current row.
		/// This may be different than FieldCount.
		/// </summary>
		public int RowFieldCount => this.curFieldCount;

		/// <inheritdoc/>
		public override int FieldCount => fieldCount;

		/// <inheritdoc/>
		public override bool HasRows => hasRows;

		/// <summary> Gets the current 1-based row number of the data reader.</summary>
		public int RowNumber => rowNumber;

		/// <inheritdoc/>
		public override bool IsClosed => state == State.Closed;

		/// <inheritdoc/>
		public override int RecordsAffected => -1;

		/// <inheritdoc/>
		public override bool GetBoolean(int ordinal)
		{
			// four cases:
			// true and false both not null. Any other value raises error.
			// true not null, false null. True string true, anything else false.
			// false not null, false null. True string true, anything else false.
			// both null. 
			var col = this.columns[ordinal];
			var trueString = col.TrueString ?? this.trueString;
			var falseString = col.FalseString ?? this.falseString;
#if NETSTANDARD2_1
			var span = this.GetFieldSpan(ordinal);
			if (trueString != null && span.Equals(trueString.AsSpan(), StringComparison.OrdinalIgnoreCase))
			{
				return true;
			}
			if (falseString != null && span.Equals(falseString.AsSpan(), StringComparison.OrdinalIgnoreCase))
			{
				return false;
			}
			if (falseString == null && trueString == null)
			{
				if (bool.TryParse(span, out bool b))
				{
					return b;
				}
				if (int.TryParse(span, out int v))
				{
					return v != 0;
				}
			}
#else
			var str = this.GetString(ordinal);
			if (trueString != null && str.Equals(trueString, StringComparison.OrdinalIgnoreCase))
			{
				return true;
			}
			if (falseString != null && str.Equals(falseString, StringComparison.OrdinalIgnoreCase))
			{
				return false;
			}
			if (falseString == null && trueString == null)
			{
				if (bool.TryParse(str, out bool b))
				{
					return b;
				}
				if (int.TryParse(str, out int v))
				{
					return v != 0;
				}
			}
#endif

			if (falseString == null && trueString != null) return false;
			if (trueString == null && falseString != null) return true;

			throw new FormatException();
		}

		/// <inheritdoc/>
		public override byte GetByte(int ordinal)
		{
#if NETSTANDARD2_1
			return byte.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return byte.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
		{
			if (buffer == null)
			{
				return GetBinaryLength(ordinal);
			}
			if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));

			var col = this.columns[ordinal];
			var encoding = col.ColumnBinaryEncoding ?? this.binaryEncoding;

			switch (encoding)
			{
				case BinaryEncoding.Base64:
					return GetBytesBase64(ordinal, (int)dataOffset, buffer, bufferOffset, length);
				case BinaryEncoding.Hexadecimal:
					return GetBytesHex(ordinal, (int)dataOffset, buffer, bufferOffset, length);
			}
			throw new NotSupportedException();// TODO: improve error message.
		}

		int GetBytesBase64(int ordinal, int dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			if (scratch == null)
			{
				scratch = new byte[3];
			}

			var offset = (int)dataOffset;

			var cs = GetField(ordinal);
			var b = cs.buffer;
			var o = cs.offset;
			var l = cs.length;

			var quadIdx = Math.DivRem(offset, 3, out int rem);

			var iBuf = b;

			var iOff = 0;
			iOff += quadIdx * 4;

			var oBuf = buffer;

			var oOff = bufferOffset;

			// align to the next base64 quad
			if (rem != 0)
			{
				FromBase64Chars(iBuf, o + iOff, 4, scratch, 0, out int c);
				if (c == rem)
				{
					// we already decoded everything available in the previous pass
					return 0; //count
				}

				Debug.Assert(c != 0); // c will be 1 2 or 3

				var partial = 3 - rem;
				while (partial > 0)
				{
					oBuf[oOff++] = scratch[3 - partial];
					c--;
					partial--;
					length--;
					if (c == 0 || length == 0)
					{
						return oOff - bufferOffset;
					}
				}

				iOff += 4; // advance the partial quad
			}

			{
				// copy as many full quads as possible straight to the output buffer
				var quadCount = Math.Min(length / 3, (l - iOff) / 4);
				if (quadCount > 0)
				{
					var charCount = quadCount * 4;
					FromBase64Chars(iBuf, o + iOff, charCount, oBuf, oOff, out int c);
					length -= c;
					iOff += charCount;
					oOff += c;
				}
			}

			if (iOff == l)
			{
				return oOff - bufferOffset;
			}

			if (length > 0)
			{
				FromBase64Chars(iBuf, o + iOff, 4, scratch, 0, out int c);
				c = length < c ? length : c;
				for (int i = 0; i < c; i++)
				{
					oBuf[oOff++] = scratch[i];
				}
			}

			return oOff - bufferOffset;
		}

		int GetBytesHex(int ordinal, int dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			var cs = GetField(ordinal);
			var b = cs.buffer;
			var o = cs.offset;

			bool hasPrefix;
			var outLen = GetHexLength(cs, out hasPrefix);
			if (hasPrefix)
			{
				o += 2;
			}

			var c = Math.Min(outLen - dataOffset, length);

			const int Invalid = 255;
						
			var bo = o;
			for (int i = 0; i < c; i++)
			{
				var cc = b[bo++];
				var v = HexValue(cc);
				if (v == Invalid)
					throw new FormatException();
				buffer[bufferOffset + i] = (byte)(v << 4);
				cc = b[bo++];
				v = HexValue(cc);
				if (v == Invalid)
					throw new FormatException();
				buffer[bufferOffset + i] |= (byte)v;
			}
			return c;
		}

		static readonly byte[] HexMap = new byte[]
			{
				255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
				255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
				255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
				  0,   1,   2,   3,   4,   5,   6,   7,   8,   9, 255, 255, 255, 255, 255, 255,
				255,  10,  11,  12,  13,  14,  15, 255, 255, 255, 255, 255, 255, 255, 255, 255,
				255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
				255,  10,  11,  12,  13,  14,  15, 255, 255, 255, 255, 255, 255, 255, 255, 255,
				255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
			};

		static int HexValue(char c)
		{
			if (c > 128) return -1;
			return HexMap[c];
		}

		void FromBase64Chars(char[] chars, int charsOffset, int charsLen, byte[] bytes, int bytesOffset, out int bytesWritten)
		{
#if NETSTANDARD2_1
			if (!Convert.TryFromBase64Chars(chars.AsSpan().Slice(charsOffset, charsLen), bytes.AsSpan().Slice(bytesOffset), out bytesWritten))
			{
				throw new FormatException();
			}
#else
			var buff = Convert.FromBase64CharArray(chars, charsOffset, charsLen);
			Array.Copy(buff, 0, bytes, bytesOffset, buff.Length);
			bytesWritten = buff.Length;
#endif
		}

		/// <inheritdoc/>
		public override char GetChar(int ordinal)
		{
			var s = GetField(ordinal);
			if (s.length == 1)
			{
				return s.buffer[s.offset];
			}
			throw new FormatException();
		}

		/// <inheritdoc/>
		public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
		{
			if (buffer == null)
			{
				return this.GetCharLength(ordinal);
			}

			if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));

			var s = GetField(ordinal);
			var len = Math.Min(s.length - dataOffset, length);

			Array.Copy(s.buffer, s.offset, buffer, bufferOffset, len);
			return len;
		}

		/// <inheritdoc/>
		public override DateTime GetDateTime(int ordinal)
		{
			var format = columns[ordinal].Format ?? this.dateFormat;
			var style = DateTimeStyles.AdjustToUniversal;
#if NETSTANDARD2_1
			if (format != null && DateTime.TryParseExact(this.GetFieldSpan(ordinal), format.AsSpan(), culture, style, out var dt))
			{
				return dt;
			}
			return DateTime.Parse(this.GetFieldSpan(ordinal), culture, style);
#else
			var dateStr = this.GetString(ordinal);
			if (format != null && DateTime.TryParseExact(dateStr, format, culture, style, out var dt))
			{
				return dt;
			}
			return DateTime.Parse(dateStr, culture, style);
#endif
		}

		/// <inheritdoc/>
		public override decimal GetDecimal(int ordinal)
		{
#if NETSTANDARD2_1
			var field = this.GetField(ordinal);
			return
				field.TryParseSingleCharInt()
				?? decimal.Parse(field.ToSpan(), provider: culture);
#else
			return decimal.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override double GetDouble(int ordinal)
		{
#if NETSTANDARD2_1
			var field = this.GetField(ordinal);
			return
				field.TryParseSingleCharInt()
				?? double.Parse(field.ToSpan(), provider: culture);
#else
			var str = this.GetString(ordinal);
			return double.Parse(str, culture);
#endif
		}

		/// <inheritdoc/>
		public override IEnumerator GetEnumerator()
		{
			return new Enumerator(this);
		}

		/// <inheritdoc/>
		public override string GetDataTypeName(int ordinal)
		{
			return this.columns[ordinal].DataTypeName!;
		}

		/// <inheritdoc/>
		public override Type GetFieldType(int ordinal)
		{
			return this.columns[ordinal].DataType!;
		}

		/// <inheritdoc/>
		public override float GetFloat(int ordinal)
		{
#if NETSTANDARD2_1
			var field = this.GetField(ordinal);
			return
				field.TryParseSingleCharInt()
				?? float.Parse(field.ToSpan(), provider: culture);
#else
			return float.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override Guid GetGuid(int ordinal)
		{
#if NETSTANDARD2_1
			return Guid.Parse(this.GetFieldSpan(ordinal));
#else
			return Guid.Parse(this.GetString(ordinal));
#endif
		}

		/// <inheritdoc/>
		public override short GetInt16(int ordinal)
		{
#if NETSTANDARD2_1
			var field = this.GetField(ordinal);
			return
				field.TryParseSingleCharInt()
				?? short.Parse(field.ToSpan(), provider: culture);
#else
			return short.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override int GetInt32(int ordinal)
		{
#if NETSTANDARD2_1

			var field = this.GetField(ordinal);

			return
				field.TryParseSingleCharInt()
				?? int.Parse(field.ToSpan(), provider: culture);
#else
			return int.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override long GetInt64(int ordinal)
		{
#if NETSTANDARD2_1
			var field = this.GetField(ordinal);
			return
				field.TryParseSingleCharInt()
				?? long.Parse(field.ToSpan(), provider: culture);
#else
			return long.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override string GetName(int ordinal)
		{
			if (ordinal < 0 || ordinal >= fieldCount)
				throw new IndexOutOfRangeException();

			return columns[ordinal].ColumnName;
		}

		/// <inheritdoc/>
		public override int GetOrdinal(string name)
		{
			if (this.headerMap.TryGetValue(name, out var idx))
			{
				if (idx == -1)
				{
					throw new AmbiguousColumnException(name);
				}
				return idx;
			}
			throw new IndexOutOfRangeException();
		}

		void ThrowIfOutOfRange(int ordinal)
		{
			if ((uint)ordinal >= (uint)fieldCount)
			{
				throw new ArgumentOutOfRangeException(nameof(ordinal));
			}
		}

		/// <inheritdoc/>
		public override string GetString(int ordinal)
		{
			if ((uint)ordinal < (uint)curFieldCount)
			{
				var s = GetFieldUnsafe(ordinal);
				var l = s.length;
				if (l == 0) return string.Empty;
				return stringFactory.Invoke(s.buffer, s.offset, l);
			}
			ThrowIfOutOfRange(ordinal);
			return string.Empty;
		}

#if NETSTANDARD2_1

		ReadOnlySpan<char> GetFieldSpan(int ordinal)
		{
			var s = GetField(ordinal);
			return s.ToSpan();
		}

#endif

		readonly struct CharSpan
		{
			public CharSpan(char[] buffer, int offset, int length)
			{
				this.buffer = buffer;
				this.offset = offset;
				this.length = length;
			}

			public char this[int idx]
			{
				get
				{
					return buffer[offset + idx];
				}
			}

			// optimization for parsing single-character numeric values
			// accessors fallback to full parser when this fails.
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			internal short? TryParseSingleCharInt()
			{
				if (length == 1)
				{
					var v = (short)(buffer[offset] - '0');
					if (v >= 0 && v < 10)
					{
						return v;
					}
				}
				return null;
			}

#if NETSTANDARD2_1

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			internal Span<char> ToSpan()
			{
				var span = buffer.AsSpan().Slice(offset, length);
				return span;
			}

#endif

			public readonly char[] buffer;
			public readonly int offset;
			public readonly int length;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		CharSpan GetField(int ordinal)
		{
			if ((uint)ordinal < (uint)this.curFieldCount)
			{
				return GetFieldUnsafe(ordinal);
			}
			// this is only called from numeric accessors
			// which would effectively be the same as trying to parse
			// an empty string.
			throw new FormatException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		CharSpan GetFieldUnsafe(int ordinal)
		{
			ref var fi = ref this.fieldInfos[ordinal];
			var startIdx = recordStart + (ordinal == 0 ? 0 : this.fieldInfos[ordinal - 1].endIdx + 1);
			var endIdx = recordStart + fi.endIdx;
			int offset = startIdx;
			int len = endIdx - startIdx;
			var buffer = this.buffer;
			if (fi.quoteState != QuoteState.Unquoted)
			{
				// if there are no escapes, we can just "trim" the quotes off
				if (fi.quoteState != QuoteState.ImplicitQuotes)
				{
					offset += 1;
					len -= 2;
				}

				if (fi.quoteState == QuoteState.Quoted && fi.escapeCount == 0)
				{
					// happy path, nothing else to do
				}
				else
				{
					return PrepareField(offset, len, fi.escapeCount);
				}
			}
			return new CharSpan(buffer, offset, len);
		}

		CharSpan PrepareField(int offset, int len, int escapeCount)
		{

			bool inQuote = true; // we start inside the quotes

			var eLen = len - escapeCount;
			// if there is room in the buffer before the current record
			// we'll use that as scratch space to unescape the value
			var temp = buffer;
			if (recordStart < eLen)
			{
				// otherwise we'll allocate a buffer
				temp = new char[eLen];
			}

			int i = 0;
			int d = 0;
			while (d < eLen)
			{
				var c = buffer[offset + i++];
				if (inQuote)
				{
					if (c == escape && i + 1 < len)
					{
						c = buffer[offset + i++];
						if (c != quote && c != escape)
						{
							if (quote == escape)
							{
								// the escape we just saw was actually the closing quote
								// the remainder of the field will be added verbatim
								inQuote = false;
							}
						}
					}
					else
					if (c == quote)
					{
						// we've found the broken closing quote
						// skip it.
						inQuote = false;
						continue;
					}
				}
				temp[d++] = c;
			}
			return new CharSpan(temp, 0, eLen);
		}

		/// <inheritdoc/>
		public override object GetValue(int ordinal)
		{
			ThrowIfOutOfRange(ordinal);

			if (columns[ordinal].AllowDBNull != false && this.IsDBNull(ordinal))
			{
				return DBNull.Value;
			}
			var type = this.GetFieldType(ordinal);

			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return this.GetBoolean(ordinal);
				case TypeCode.Char:
					return this.GetChar(ordinal);
				case TypeCode.Byte:
					return this.GetByte(ordinal);
				case TypeCode.Int16:
					return this.GetInt16(ordinal);
				case TypeCode.Int32:
					return this.GetInt32(ordinal);
				case TypeCode.Int64:
					return this.GetInt64(ordinal);
				case TypeCode.Single:
					return this.GetFloat(ordinal);
				case TypeCode.Double:
					return this.GetDouble(ordinal);
				case TypeCode.Decimal:
					return this.GetDecimal(ordinal);
				case TypeCode.DateTime:
					return this.GetDateTime(ordinal);
				case TypeCode.String:
					return this.GetString(ordinal);
				default:
					if (type == typeof(byte[]))
					{
						var length = this.GetBinaryLength(ordinal);
						var buffer = new byte[length];
						var len = GetBytes(ordinal, 0, buffer, 0, length);
						Debug.Assert(len == length);
						return buffer;
					}
					if (type == typeof(Guid))
					{
						return this.GetGuid(ordinal);
					}
					return this.GetString(ordinal);
			}
		}

		int GetBinaryLength(int ordinal)
		{
			var span = this.GetField(ordinal);
			var col = this.columns[ordinal];

			var enc = col.ColumnBinaryEncoding ?? this.binaryEncoding;
			switch (enc)
			{
				case BinaryEncoding.Base64:
					return GetBase64Length(span);
				case BinaryEncoding.Hexadecimal:
					return GetHexLength(span, out _);
			}
			throw new NotSupportedException(); // TODO: improve error message.
		}

		static int GetBase64Length(CharSpan span)
		{
			var l = span.length;
			// must be divisible by 4
			if (l % 4 != 0) throw new FormatException();
			var rem = 0;
			if (span[l - 1] == '=') rem++;
			if (span[l - 2] == '=') rem++;
			var dataLen = l / 4 * 3;
			dataLen -= rem;
			return dataLen;
		}

		static int GetHexLength(CharSpan span, out bool hasPrefix)
		{
			hasPrefix = false;
			var l = span.length;
			// must be divisible by 1
			if (l % 2 != 0) throw new FormatException();
			if (l >= 2 && char.ToLowerInvariant(span[1]) == 'x' && span[0] == '0')
			{
				hasPrefix = true;
				l -= 2;
			}
			return l / 2;
		}

		int GetCharLength(int ordinal)
		{
			var fi = this.fieldInfos[ordinal];
			var start = ordinal == 0 ? 0 : this.fieldInfos[ordinal - 1].endIdx + 1;
			var len = fi.endIdx - start - fi.escapeCount;
			return len;
		}

		/// <inheritdoc/>
		public override int GetValues(object?[] values)
		{
			var count = Math.Min(this.fieldCount, values.Length);
			for (int i = 0; i < count; i++)
			{
				values[i] = GetValue(i);
			}
			return count;
		}

		/// <inheritdoc/>
		public override bool IsDBNull(int ordinal)
		{
			ThrowIfOutOfRange(ordinal);
			var col = columns[ordinal];

			if (col.AllowDBNull == false)
			{
				// if the schema claims it is not nullable then we will honor that claim
				// if the schema is wrong a FormatException or CastException may result
				// when trying to access the value.
				return false;
			}

			if ((uint)ordinal >= (uint)curFieldCount)
			{
				// if the current row has missing fields, consider them null
				return true;
			}

			// now pay the cost of determining if the thing is null.
			var infos = fieldInfos;
			ref var fi = ref infos[ordinal];

			if (fi.quoteState != QuoteState.Unquoted)
			{
				// never consider quoted fields as null
				return false;
			}

			var startIdx = recordStart + (ordinal == 0 ? 0 : infos[ordinal - 1].endIdx + 1);
			var endIdx = recordStart + fi.endIdx;
			var buf = this.buffer;

			// if the entire field is whitespace, consider it null.
			// The default configuration will never get here, because
			// all strings are considered non-null by default, so
			// this method will have exited early with false.
			for (int i = startIdx; i < endIdx; i++)
			{
				if (char.IsWhiteSpace(buf[i]) == false)
					return false;
			}
			return true;
		}

		/// <inheritdoc/>
		public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
		{
			return IsDBNull(ordinal) ? CompleteTrue : CompleteFalse;
		}

		readonly static Task<bool> CompleteTrue = Task.FromResult(true);
		readonly static Task<bool> CompleteFalse = Task.FromResult(false);

		/// <summary>
		/// Gets a collection of DbColumns describing the schema of the data reader.
		/// </summary>
		/// <returns>A collection of DbColumn.</returns>
		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			// I expect that callers would only call this once, so no bother caching.
			return new ReadOnlyCollection<DbColumn>(columns);
		}

		/// <inheritdoc/>
		public override DataTable GetSchemaTable()
		{
			return SchemaTable.GetSchemaTable(this.GetColumnSchema());
		}

		class CsvColumn : DbColumn
		{
			public string? Format { get; }

			// encoding specified at the column level, fallback to the options-spec if null
			public BinaryEncoding? ColumnBinaryEncoding { get; }

			public string? TrueString { get; }

			public string? FalseString { get; }

			public CsvColumn(string? name, int ordinal, DbColumn? schema = null)
			{
				// non-overridable
				this.ColumnOrdinal = ordinal;
				this.IsReadOnly = true; // I don't understand what false would mean here.

				var colName = schema?.ColumnName;

				this.ColumnName = string.IsNullOrEmpty(colName) ? name ?? "" : colName;
				this.DataType = schema?.DataType ?? typeof(string);
				this.DataTypeName = schema?.DataTypeName ?? this.DataType.Name;

				// by default, we don't consider string types to be nullable,
				// an empty field for a string means "", not null.
				this.AllowDBNull = schema == null ? false : schema?.AllowDBNull;

				this.ColumnSize = schema?.ColumnSize ?? int.MaxValue;

				this.IsUnique = schema?.IsUnique ?? false;
				this.IsLong = schema?.IsLong ?? false;
				this.IsKey = schema?.IsKey ?? false;
				this.IsIdentity = schema?.IsIdentity ?? false;
				this.IsHidden = schema?.IsHidden ?? false;
				this.IsExpression = schema?.IsExpression ?? false;
				this.IsAutoIncrement = schema?.IsAutoIncrement ?? false;
				this.NumericPrecision = schema?.NumericPrecision;
				this.NumericScale = schema?.NumericScale;
				this.IsAliased = schema?.IsAliased ?? false;
				this.BaseTableName = schema?.BaseTableName;
				this.BaseServerName = schema?.BaseServerName;
				this.BaseSchemaName = schema?.BaseSchemaName;
				this.BaseColumnName = schema?.BaseColumnName ?? name; // default in the orignal header name if they chose to remap it.
				this.BaseCatalogName = schema?.BaseCatalogName;
				this.UdtAssemblyQualifiedName = schema?.UdtAssemblyQualifiedName;
				this.Format = schema?[nameof(Format)] as string;
				if (this.DataType == typeof(byte[]))
				{
					this.ColumnBinaryEncoding = GetBinaryEncoding(this.Format);
				}
				if (this.DataType == typeof(bool) && this.Format != null)
				{
					var idx = this.Format.IndexOf("|");
					if (idx == -1)
					{
						this.TrueString = this.Format;
					}
					else
					{
						this.TrueString = this.Format.Substring(0, idx);
						this.TrueString = this.TrueString.Length == 0 ? null : this.TrueString;
						this.FalseString = this.Format.Substring(idx + 1);
						this.FalseString = this.FalseString.Length == 0 ? null : this.FalseString;
					}
				}
			}

			BinaryEncoding? GetBinaryEncoding(string? format)
			{
				if (format == null)
					return null;
				if (StringComparer.OrdinalIgnoreCase.Equals("base64", format))
					return BinaryEncoding.Base64;
				if (StringComparer.OrdinalIgnoreCase.Equals("hex", format))
					return BinaryEncoding.Hexadecimal;

				// for unknown encoding spec, allow initialize but any access
				// to the column as a binary value will produce a NotSupportedException.
				return 0;
			}

			/// <inheritdoc/>
			public override object? this[string property]
			{
				get
				{
					switch (property)
					{
						case nameof(Format):
							return Format;
						default:
							return base[property];
					}
				}
			}
		}

#if NETSTANDARD2_1

		/// <summary>
		/// Gets a span containing the current record data, including the line ending.
		/// </summary>
		public ReadOnlySpan<char> GetRawRecordSpan()
		{
			var len = this.idx - this.recordStart;
			return this.buffer.AsSpan().Slice(this.recordStart, len);
		}

#endif

		/// <summary>
		/// Copies the raw record data from the buffer.
		/// </summary>
		/// <param name="buffer">The buffer to receive the data, or null to query the required size.</param>
		/// <param name="offset">The offset into the buffer to start writing.</param>
		/// <returns>The length of the record data.</returns>
		public int GetRawRecord(char[]? buffer, int offset)
		{
			var len = this.idx - this.recordStart;
			if (buffer != null)
			{
				if (offset < 0 || offset >= buffer.Length)
					throw new ArgumentOutOfRangeException(nameof(offset));

				if (buffer.Length - offset < len)
					throw new ArgumentOutOfRangeException(nameof(buffer));

				Array.Copy(this.buffer, this.recordStart, buffer, offset, len);
			}
			return len;
		}
	}
}
