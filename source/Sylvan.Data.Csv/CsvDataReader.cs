using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// A data reader for delimited text data.
	/// </summary>
	public sealed class CsvDataReader : DbDataReader, IDbColumnSchemaGenerator
	{
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
		}

		struct FieldInfo
		{
			internal int endIdx;
			internal int escapeCount;
			internal QuoteState isQuoted;

#if DEBUG
			public override string ToString()
			{
				return $"EndIdx: {endIdx} IsQuoted: {isQuoted} EscapeCount: {escapeCount}";
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

		readonly char delimiter;
		readonly char quote;
		readonly char escape;
		readonly bool ownsReader;

		readonly CultureInfo culture;

		readonly TextReader reader;
		bool hasRows;

		int recordStart;
		int bufferEnd;
		int idx;

		int fieldCount; // fields in the header (or firstRow)

		int curFieldCount; // fields in current row

		int rowNumber;
		int lineNumber;

		readonly char[] buffer;
		byte[]? scratch;
		FieldInfo[] fieldInfos;

		bool atEndOfText;
		readonly bool hasHeaders;
		readonly Dictionary<string, int> headerMap;

		CsvColumn[] columns;
		State state;

		/// <summary>
		/// Creates a new CsvDataReader.
		/// </summary>
		/// <param name="reader">The TextReader for the delimited data.</param>
		/// <param name="options">The options to configure the reader, or null to use the default options.</param>
		/// <returns>A CsvDataReader instance.</returns>
		public static CsvDataReader Create(TextReader reader, CsvDataReaderOptions? options = null)
		{
			return CreateAsync(reader, options).GetAwaiter().GetResult();
		}

		/// <summary>
		/// Creates a new CsvDataReader asynchronously.
		/// </summary>
		/// <param name="reader">The TextReader for the delimited data.</param>
		/// <param name="options">The options to configure the reader, or null to use the default options.</param>
		/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
		public static async Task<CsvDataReader> CreateAsync(TextReader reader, CsvDataReaderOptions? options = null)
		{
			if (reader == null) throw new ArgumentNullException(nameof(reader));
			var csv = new CsvDataReader(reader, options);
			await csv.InitializeAsync(options?.Schema);
			return csv;
		}

		private CsvDataReader(TextReader reader, CsvDataReaderOptions? options = null)
		{
			if (options != null)
				options.Validate();
			options ??= CsvDataReaderOptions.Default;
			this.reader = reader;
			this.buffer = new char[options.BufferSize];

			this.hasHeaders = options.HasHeaders;
			this.delimiter = options.Delimiter;
			this.quote = options.Quote;
			this.escape = options.Escape;

			this.recordStart = 0;
			this.bufferEnd = 0;
			this.idx = 0;
			this.rowNumber = 0;
			this.lineNumber = 0;
			this.fieldInfos = new FieldInfo[16];
			this.headerMap = new Dictionary<string, int>(options.HeaderComparer);
			this.columns = Array.Empty<CsvColumn>();
			this.culture = options.Culture;
			this.ownsReader = options.OwnsReader;
		}

		async Task InitializeAsync(ICsvSchemaProvider? schema)
		{
			state = State.Initializing;
			this.lineNumber = 1;
			// if the user specified that there are headers
			// read them, and use them to determine fieldCount.
			if (hasHeaders)
			{
				if (await NextRecordAsync())
				{
					InitializeSchema(schema);
				}
				else
				{
					throw new CsvMissingHeadersException();
				}
			}

			// read the first row of data to determine fieldCount (if there were no headers)
			// and support calling HasRows before Read is first called.
			this.hasRows = await NextRecordAsync();
			InitializeSchema(schema);
		}

		void InitializeSchema(ICsvSchemaProvider? schema)
		{
			if (state != State.Initializing) return;

			columns = new CsvColumn[this.fieldCount];
			for (int i = 0; i < this.fieldCount; i++)
			{
				var name = hasHeaders ? GetString(i) : null;
				var columnSchema = schema?.GetColumn(name, i);
				columns[i] = new CsvColumn(name, i, columnSchema);

				name = columns[i].ColumnName;
				if (name != null)
				{
					headerMap.Add(name, i);
				}
			}
			this.state = State.Initialized;
		}

		async Task<bool> NextRecordAsync()
		{
			this.curFieldCount = 0;
			this.recordStart = this.idx;

			if (this.idx >= bufferEnd)
			{
				await FillBufferAsync();
				if (idx == bufferEnd)
				{
					return false;
				}
			}

			int fieldIdx = 0;
			while (true)
			{
				var result = ReadField(fieldIdx);

				if (result == ReadResult.True)
				{
					fieldIdx++;
					continue;
				}
				if (result == ReadResult.False)
				{
					return true;
				}
				if (result == ReadResult.Incomplete)
				{
					// we were unable to read an entire record out of the buffer synchronously
					if (recordStart == 0)
					{
						// if we consumed the entire buffer reading this record, then this is an exceptional situation
						// we expect a record to be able to fit entirely within the buffer.
						throw new CsvRecordTooLargeException(this.RowNumber, fieldIdx, null, null);
					}
					else
					{
						await FillBufferAsync();
						// after filling the buffer, we will resume reading fields from where we left off.
					}
				}
			}
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

			if (idx >= bufferEnd)
			{
				return ReadResult.Incomplete;
			}
			else
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
								{
									if (escape == quote)
									{
										idx--;
										closeQuoteIdx = idx;
										// the quote (escape) we just saw was a the closing quote
										break;
									}
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
							var r = ConsumeLineEnd(ref idx);
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
					var r = ConsumeLineEnd(ref idx);
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
				if (state == State.Initializing)
				{
					if (fieldIdx >= fieldInfos.Length)
					{
						// this resize is constrained by the fact that the record has to fit in one row
						Array.Resize(ref fieldInfos, fieldInfos.Length * 2);
					}
					fieldCount++;
				}
				if (fieldIdx < fieldCount)
				{
					curFieldCount++;
					ref var fi = ref fieldInfos[fieldIdx];
					fi.isQuoted =
						closeQuoteIdx == -1
						? QuoteState.Unquoted
						: fieldEnd == (closeQuoteIdx - recordStart)
						? QuoteState.Quoted
						: QuoteState.BrokenQuotes;
					fi.escapeCount = escapeCount;
					fi.endIdx = complete ? fieldEnd : (idx - recordStart);
				}
				this.idx = idx;

				if (complete)
					return last ? ReadResult.False : ReadResult.True;

				return ReadResult.False;
			}

			return ReadResult.Incomplete;
		}

		static bool IsEndOfLine(char c)
		{
			return c == '\r' || c == '\n';
		}

		ReadResult ConsumeLineEnd(ref int idx)
		{
			lineNumber++;
			var c = buffer[idx++];
			if (c == '\r')
			{
				if (idx < buffer.Length)
				{
					c = buffer[idx++];
					if (c == '\n')
					{
						return ReadResult.True;
					}
					else
					{
						// weird, but we'll allow a lone \r
						idx--;
						return ReadResult.True;
					}
				}
				else
				{
					if (atEndOfText)
					{
						return ReadResult.True;
					}
					// the next buffer might contain a \n
					// that we need to consume.
					lineNumber--;
					return ReadResult.Incomplete;
				}
			}
			if (c == '\n')
			{
				return ReadResult.True;
			}
			// this can never be reached.
			return ReadResult.False;
		}

		async Task<int> FillBufferAsync()
		{
			if (recordStart != 0)
			{
				// move any pending data to the front of the buffer.
				Array.Copy(buffer, recordStart, buffer, 0, bufferEnd - recordStart);
			}

			bufferEnd -= recordStart;
			idx -= recordStart;
			recordStart = 0;

			var count = buffer.Length - bufferEnd;
			var c = await reader.ReadBlockAsync(buffer, bufferEnd, count);
			bufferEnd += c;
			if (c < count)
			{
				atEndOfText = true;
			}
			return c;
		}

		/// <inheritdoc/>
		public override object? this[int ordinal] => this.GetValue(ordinal);

		/// <inheritdoc/>
		public override object? this[string name] => this[this.GetOrdinal(name)];

		/// <inheritdoc/>
		public override int Depth => 0;

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
#if NETSTANDARD2_1
			return bool.Parse(this.GetFieldSpan(ordinal));
#else
			return bool.Parse(this.GetString(ordinal));
#endif
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
		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));

			if (scratch == null)
			{
				scratch = new byte[3];
			}

			var offset = (int)dataOffset;

			var (b, o, l) = GetField(ordinal);

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
			var (b, o, l) = GetField(ordinal);
			if (l == 1)
			{
				return b[o];
			}
			throw new FormatException();
		}

		/// <inheritdoc/>
		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));

			var (b, o, l) = GetField(ordinal);
			var len = Math.Min(l - dataOffset, length);

			Array.Copy(b, o, buffer, bufferOffset, len);
			return len;
		}

		/// <inheritdoc/>
		public override DateTime GetDateTime(int ordinal)
		{
#if NETSTANDARD2_1
			return DateTime.Parse(this.GetFieldSpan(ordinal), culture);
#else
			return DateTime.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override decimal GetDecimal(int ordinal)
		{
#if NETSTANDARD2_1
			return decimal.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return decimal.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override double GetDouble(int ordinal)
		{
#if NETSTANDARD2_1
			return double.Parse(this.GetFieldSpan(ordinal), provider: culture);
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
			return this.columns[ordinal].DataTypeName;
		}

		/// <inheritdoc/>
		public override Type GetFieldType(int ordinal)
		{
			return this.columns[ordinal].DataType;
		}

		/// <inheritdoc/>
		public override float GetFloat(int ordinal)
		{
#if NETSTANDARD2_1
			return float.Parse(this.GetFieldSpan(ordinal), provider: culture);
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
			return short.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return short.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override int GetInt32(int ordinal)
		{
#if NETSTANDARD2_1
			return int.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return int.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override long GetInt64(int ordinal)
		{
#if NETSTANDARD2_1
			return long.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return long.Parse(this.GetString(ordinal), culture);
#endif
		}

		/// <inheritdoc/>
		public override string GetName(int ordinal)
		{
			if (ordinal < 0 || ordinal >= fieldCount) 
				throw new IndexOutOfRangeException();

			return columns[ordinal].ColumnName ?? "";
		}

		/// <inheritdoc/>
		public override int GetOrdinal(string name)
		{
			if (this.headerMap.TryGetValue(name, out var idx))
			{
				return idx;
			}
			throw new IndexOutOfRangeException();
		}

		/// <inheritdoc/>
		public override string GetString(int ordinal)
		{
			if (ordinal >= 0 && ordinal < curFieldCount)
			{
				var (b, o, l) = GetField(ordinal);
				return l == 0 ? string.Empty : new string(b, o, l);
			}
			return string.Empty;
		}

#if NETSTANDARD2_1
		ReadOnlySpan<char> GetFieldSpan(int ordinal)
		{
			var (b, o, l) = GetField(ordinal);
			return b.AsSpan().Slice(o, l);
		}
#endif
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		(char[] buffer, int offset, int len) GetField(int ordinal)
		{
			ref var fi = ref this.fieldInfos[ordinal];
			var startIdx = recordStart + (ordinal == 0 ? 0 : this.fieldInfos[ordinal - 1].endIdx + 1);
			var endIdx = recordStart + fi.endIdx;
			int offset = startIdx;
			int len = endIdx - startIdx;
			var buffer = this.buffer;
			if (fi.isQuoted != QuoteState.Unquoted)
			{
				// if there are no escapes, we can just "trim" the quotes off
				offset += 1;
				len -= 2;

				if (fi.isQuoted == QuoteState.Quoted && fi.escapeCount == 0)
				{
					// happy path, nothing else to do
				}
				else
				{
					bool inQuote = true; // we start inside the quotes

					var eLen = len - fi.escapeCount;
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
					while (d < len)
					{
						var c = buffer[offset + i++];
						if (inQuote)
						{
							if (c == escape)
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
					buffer = temp;
					offset = 0;
					len = eLen;
				}
			}
			return (buffer, offset, len);
		}

		/// <inheritdoc/>
		public override object? GetValue(int ordinal)
		{
			if ((uint)ordinal >= fieldCount)
				throw new ArgumentOutOfRangeException(nameof(ordinal));

			if (columns[ordinal].AllowDBNull == true && this.IsDBNull(ordinal))
			{
				return null;
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
						var (b, o, l) = this.GetField(ordinal);
						var dataLen = l / 4 * 3;
						var buffer = new byte[dataLen];
						var len = GetBytes(ordinal, 0, buffer, 0, dataLen);
						// 2/3 chance we'll have to resize.
						if (len < dataLen)
						{
							Array.Resize(ref buffer, (int)len);
						}
						return buffer;
					}
					if (type == typeof(Guid))
					{
						return this.GetGuid(ordinal);
					}
					return this.GetString(ordinal);
			}
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
			if (((uint)ordinal) >= fieldCount)
				throw new ArgumentOutOfRangeException(nameof(ordinal));
			if (ordinal >= curFieldCount)
			{
				return true;
			}
			var col = columns[ordinal];
			if (col.DataType == typeof(string))
			{
				// empty string fields will be treated as empty string, not null.
				return false;
			}
			// now pay the cost of determining if the thing is null.
			ref var fi = ref fieldInfos[ordinal];
			var startIdx = recordStart + (ordinal == 0 ? 0 : this.fieldInfos[ordinal - 1].endIdx + 1);
			var endIdx = recordStart + fi.endIdx;
			var isEmpty = endIdx - startIdx - (fi.isQuoted != QuoteState.Unquoted ? 2 : 0) - fi.escapeCount == 0;
			return isEmpty;
		}

		/// <inheritdoc/>
		public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
		{
			return IsDBNull(ordinal) ? CompleteTrue : CompleteFalse;
		}

		/// <inheritdoc/>
		public override bool NextResult()
		{
			state = State.End;
			return false;
		}

		/// <inheritdoc/>
		public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
		{
			state = State.End;
			return CompleteFalse;
		}

		readonly static Task<bool> CompleteTrue = Task.FromResult(true);
		readonly static Task<bool> CompleteFalse = Task.FromResult(false);

		/// <inheritdoc/>
		public override Task<bool> ReadAsync(CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();
			this.rowNumber++;
			switch (state)
			{
				case State.Initialized:
					state = State.Open;
					// after initizialization, the first record would already be in the buffer
					// if hasRows is true.
					if (hasRows)
					{
						return CompleteTrue;
					}
					else
					{
						this.rowNumber = -1;
						this.state = State.End;
						return CompleteFalse;
					}
				case State.Open:
					return this.NextRecordAsync();
				case State.End:
					this.rowNumber = -1;
					return CompleteFalse;
			}
			throw new InvalidOperationException();
		}

		/// <inheritdoc/>
		public override bool Read()
		{
			return this.ReadAsync().GetAwaiter().GetResult();
		}

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
			public CsvColumn(string? name, int ordinal, DbColumn? schema = null)
			{
				// non-overridable
				this.ColumnOrdinal = ordinal;
				this.IsReadOnly = true; // I don't understand what false would mean here.

				this.ColumnName = schema?.ColumnName ?? name;
				this.DataType = schema?.DataType ?? typeof(string);
				this.DataTypeName = schema?.DataTypeName ?? this.DataType.Name;

				// by default, we don't consider string types to be nullable,
				// an empty field for a string means "" not null.
				this.AllowDBNull = schema?.AllowDBNull ?? this.DataType.IsValueType;

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
			}
		}

		/// <inheritdoc/>
		public override void Close()
		{
			if (this.state != State.Closed)
			{
				if (ownsReader)
					this.reader.Dispose();
				this.state = State.Closed;
			}
		}

#if NETSTANDARD2_1

		/// <inheritdoc/>
		public override Task CloseAsync()
		{
			if (this.state != State.Closed)
			{
				if (ownsReader)
					this.reader.Dispose();
				this.state = State.Closed;
			}
			return Task.CompletedTask;
		}

#endif

	}
}
