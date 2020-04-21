using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
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

		struct FieldInfo
		{
			internal int endIdx;
			internal int escapeCount;
			internal bool isQuoted;

			public override string ToString()
			{
				return $"EndIdx: {endIdx} IsQuoted: {isQuoted} EscapeCount: {escapeCount}";
			}
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
		FieldInfo[] fieldInfos;

		bool atEndOfText;
		readonly bool hasHeaders;
		readonly Dictionary<string, int> headerMap;

		CsvColumn[] columns;
		State state;

		public static CsvDataReader Create(TextReader reader, CsvDataReaderOptions? options = null)
		{
			return CreateAsync(reader, options).Result;
		}

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
					throw new InvalidDataException("no headers");
				}
			}
			// read the first row of data to determine fieldCount (if there were no headers)
			// and support "hasRows" before Read.
			this.hasRows = await NextRecordAsync();
			InitializeSchema(schema);

			if (state == State.End)
			{
				// in the event there is only one line in the file
				state = State.Initialized;
			}
		}

		void InitializeSchema(ICsvSchemaProvider? schema)
		{
			if (state != State.Initializing) return;

			columns = new CsvColumn[this.fieldCount];
			for (int i = 0; i < this.fieldCount; i++)
			{
				var name = GetString(i);
				var columnSchema = schema?.GetColumn(name, i);
				columns[i] = new CsvColumn(name, i, columnSchema);

				headerMap.Add(name, i);
			}

			state = State.Initialized;
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
						throw new CsvRecordTooLargeException(lineNumber);
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
			bool isQuoted = false;
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
					isQuoted = true;
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
					fi.isQuoted = isQuoted;
					fi.escapeCount = escapeCount;
					fi.endIdx = complete ? fieldEnd : (idx - recordStart);
				}
				this.idx = idx;

				if (complete)
					return last ? ReadResult.False : ReadResult.True;
				this.state = State.End;

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

		public override object? this[int ordinal] => this.GetValue(ordinal);

		public override object? this[string name] => this[this.GetOrdinal(name)];

		public override int Depth => 0;

		public override int FieldCount => fieldCount;

		public override bool HasRows => hasRows;

		public int RowNumber => rowNumber;

		public override bool IsClosed => state == State.Closed;

		public override int RecordsAffected => -1;

		public override bool GetBoolean(int ordinal)
		{
#if NETSTANDARD2_1
			return bool.Parse(this.GetFieldSpan(ordinal));
#else
			return bool.Parse(this.GetString(ordinal));
#endif
		}

		public override byte GetByte(int ordinal)
		{
#if NETSTANDARD2_1
			return byte.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return byte.Parse(this.GetString(ordinal), culture);
#endif
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			throw new NotSupportedException();
		}

		public override char GetChar(int ordinal)
		{
			var (b, o, l) = GetField(ordinal);
			if (l == 1)
			{
				return b[o];
			}
			throw new FormatException();
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));

			var (b, o, l) = GetField(ordinal);
			var len = Math.Min(l - dataOffset, length);
			
			Array.Copy(b, o, buffer, bufferOffset, len);
			return len;
		}

		public override DateTime GetDateTime(int ordinal)
		{
#if NETSTANDARD2_1
			return DateTime.Parse(this.GetFieldSpan(ordinal), culture);
#else
			return DateTime.Parse(this.GetString(ordinal), culture);
#endif
		}

		public override decimal GetDecimal(int ordinal)
		{
#if NETSTANDARD2_1
			return decimal.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return decimal.Parse(this.GetString(ordinal), culture);
#endif
		}

		public override double GetDouble(int ordinal)
		{
#if NETSTANDARD2_1
			return double.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			var str = this.GetString(ordinal);
			return double.Parse(str, culture);
#endif
		}

		public override IEnumerator GetEnumerator()
		{
			return new Enumerator(this);
		}

		public override string GetDataTypeName(int ordinal)
		{
			return this.columns[ordinal].DataTypeName;
		}

		public override Type GetFieldType(int ordinal)
		{
			return this.columns[ordinal].DataType;
		}

		public override float GetFloat(int ordinal)
		{
#if NETSTANDARD2_1
			return float.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return float.Parse(this.GetString(ordinal), culture);
#endif
		}

		public override Guid GetGuid(int ordinal)
		{
#if NETSTANDARD2_1
			return Guid.Parse(this.GetFieldSpan(ordinal));
#else
			return Guid.Parse(this.GetString(ordinal));
#endif
		}

		public override short GetInt16(int ordinal)
		{
#if NETSTANDARD2_1
			return short.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return short.Parse(this.GetString(ordinal), culture);
#endif
		}

		public override int GetInt32(int ordinal)
		{
#if NETSTANDARD2_1
			return int.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return int.Parse(this.GetString(ordinal), culture);
#endif
		}

		public override long GetInt64(int ordinal)
		{
#if NETSTANDARD2_1
			return long.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
			return long.Parse(this.GetString(ordinal), culture);
#endif
		}

		public override string GetName(int ordinal)
		{
			if (this.hasHeaders == false) throw new InvalidOperationException();
			return columns[ordinal].ColumnName;
		}

		public override int GetOrdinal(string name)
		{
			if (this.hasHeaders == false) throw new InvalidOperationException();
			return this.headerMap.TryGetValue(name, out var idx) ? idx : -1;
		}

		public override string GetString(int ordinal)
		{
			if (((uint)ordinal) < curFieldCount)
			{
				var (b, o, l) = GetField(ordinal);
				return new string(b, o, l);
			}
			return "";
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
			if (fi.isQuoted)
			{
				offset += 1;
				len -= 2;
				if (fi.escapeCount > 0)
				{
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
					while (i < len)
					{
						var c = buffer[offset + i++];
						if (c == escape)
						{
							c = buffer[offset + i++];
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

		public override object? GetValue(int ordinal)
		{
			if (columns[ordinal].AllowDBNull == true && this.IsDBNull(ordinal))
			{
				return null;
			}
			var type = this.GetFieldType(ordinal);

			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return this.GetBoolean(ordinal);
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
				default:
					return this.GetString(ordinal);
			}
		}

		public override int GetValues(object?[] values)
		{
			var count = Math.Min(this.fieldCount, values.Length);
			for (int i = 0; i < count; i++)
			{
				values[i] = GetValue(i);
			}
			return count;
		}

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
			var isEmpty = endIdx - startIdx - (fi.isQuoted ? 2 : 0) - fi.escapeCount == 0;
			if (col.DataType == typeof(DateTime) && this.GetString(ordinal) == "")
			{
				;
			}
			return isEmpty;
		}

		public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
		{
			return IsDBNull(ordinal) ? CompleteTrue : CompleteFalse;
		}

		public override bool NextResult()
		{
			state = State.End;
			return false;
		}

		public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
		{
			state = State.End;
			return CompleteFalse;
		}

		readonly static Task<bool> CompleteTrue = Task.FromResult(true);
		readonly static Task<bool> CompleteFalse = Task.FromResult(false);

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

		public override bool Read()
		{
			return this.ReadAsync().Result;
		}

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			// I expect that callers would only call this once, so no bother caching.
			return new ReadOnlyCollection<DbColumn>(columns);
		}

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
	}
}
