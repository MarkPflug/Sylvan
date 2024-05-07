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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if INTRINSICS
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
#endif

namespace Sylvan.Data.Csv;

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
		ImplicitQuotes = 3,
		InvalidQuotes = 4,
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
		// the initial state when the reader is created.
		None = 0,
		// the state when the reader is initialized
		// meaning that the next record is already in the buffer
		// so the next call to Read will be a no-op.
		Initialized,
		// the normal state when the next call to Read will process the next record.
		Open,
		// the state when the end of the record set is reached
		// not necessarily the end of the file when in multi-result set mode.
		End,
		// the state when the reader has bee closed/disposed.
		Closed,
	}

	void ValidateState()
	{
		if (this.state != State.Open)
		{
			throw new InvalidOperationException();
		}
	}

	enum ReadResult
	{
		False,
		True,
		Incomplete,
	}

	readonly TextReader reader;
	bool hasRows;
	char[] buffer;

	readonly int maxBufferSize;
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

	struct OrdinalCache
	{
		internal string name;
		internal int idx;
	}

	OrdinalCache[] colCache = Array.Empty<OrdinalCache>();
	int colCacheIdx;

	// An exception that was created with initializing, and should be thrown on the next call to Read/ReadAsync
	Exception? pendingException;

	// in multi-result set mode carryRow indicates that a row is already parsed
	// and needs to be carried into the next result set.
	bool carryRow;

	readonly Dictionary<string, int> headerMap;

	readonly bool autoDetectDelimiter;
	// options:
	char delimiter;
	readonly CsvStyle style;
	readonly char quote;
	readonly char escape;
	readonly char comment;
	char minSafe;
	NewLineMode newLineMode;
	readonly bool ownsReader;
	readonly CultureInfo culture;
	readonly string? dateTimeFormat;
	readonly string? trueString, falseString;
	readonly BinaryEncoding binaryEncoding;
	readonly bool hasHeaders;
	readonly ColumnStringFactory stringFactory;
	readonly CommentHandler? commentHandler;
	readonly ICsvSchemaProvider? schema;
	readonly ResultSetMode resultSetMode;

	static int GetBufferSize(TextReader reader, CsvDataReaderOptions options)
	{
		var bufferLen = options.BufferSize;
		// utf8 bytes can only get shorter when converted to characters
		// so if we can determine an underlying stream length, then we can allocate
		// a smaller buffer.
		if (reader is StreamReader sr && sr.CurrentEncoding.CodePage == Encoding.UTF8.CodePage)
		{
			var s = sr.BaseStream;
			if (s.CanSeek && s.Length < bufferLen)
			{
				// allocate an extra byte, which allows detecting
				// that the end of stream is reached.
				bufferLen = (int)s.Length + 1;
			}
		}
		return bufferLen;
	}

	static int GetIOBufferSize(CsvDataReaderOptions? options)
	{
		return (options?.BufferSize ?? CsvDataReaderOptions.Default.BufferSize);
	}

	private CsvDataReader(TextReader reader, char[]? buffer, CsvDataReaderOptions options)
	{
		options.Validate();
		this.reader = reader;
		var bufferLen = GetBufferSize(reader, options);
#pragma warning disable CS0618 // Type or member is obsolete
		this.buffer = buffer ?? options.Buffer ?? new char[bufferLen];
#pragma warning restore CS0618 // Type or member is obsolete

		this.hasHeaders = options.HasHeaders;
		this.autoDetectDelimiter = options.Delimiter == null;
		this.delimiter = options.Delimiter ?? '\0';
		this.style = options.CsvStyle;
		this.quote = options.Quote;
		this.escape = options.Escape;
		this.comment = options.Comment;

		this.dateTimeFormat = options.DateTimeFormat;
#if NET6_0_OR_GREATER
		this.dateOnlyFormat = options.DateOnlyFormat;
#endif
		this.trueString = options.TrueString;
		this.falseString = options.FalseString;

		this.maxBufferSize = options.MaxBufferSize ?? -1;
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
		this.stringFactory = BuildStringFactory(options.ColumnStringFactory, options.StringFactory);

		this.commentHandler = options.CommentHandler;
		this.schema = options.Schema;
		this.resultSetMode = options.ResultSetMode;
		this.newLineMode = NewLineMode.Unknown;
	}

	static ColumnStringFactory BuildStringFactory(ColumnStringFactory? csf, StringFactory? sf)
	{
		if (csf != null)
		{
			if (sf != null)
			{
				return
					new ColumnStringFactory(
						(r, i, b, o, l) =>
						{
							return csf(r, i, b, o, l) ?? sf(b, o, l);
						}
					);
			}
			else
			{
				return (r, i, b, o, l) => csf(r, i, b, o, l);
			}
		}
		else
		if (sf != null)
		{
			return (r, i, b, o, l) => sf(b, o, l);
		}
		return (r, i, b, o, l) => null;
	}

	enum NewLineMode
	{
		Unknown,
		Standard,
		MacOS,
	}

	char DetectDelimiter()
	{
		int[] counts = new int[AutoDetectDelimiters.Length];
		for (int i = recordStart; i < bufferEnd; i++)
		{
			var c = buffer[i];
			if (c == '\n' || c == '\r')
			{
				if (c == '\r' && i + 1 < buffer.Length && buffer[i + 1] != '\n')
				{
					newLineMode = NewLineMode.MacOS;
				}
				var x = counts.Sum();
				if (x == 0)
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

	NewLineMode DetectNewLine()
	{
		// This method should only be called when the delimiter was explicitly set by the user.
		// In which case we need to detect whether the newline characters are MacOS-style ('\r')
		for (int i = recordStart; i < bufferEnd; i++)
		{
			var c = buffer[i];
			if (c == '\n')
			{
				return NewLineMode.Standard;
			}

			if (c == '\r')
			{
				if (i + 1 < buffer.Length)
				{
					return buffer[i + 1] == '\n'
						? NewLineMode.Standard
						: NewLineMode.MacOS;
				}
			}
		}
		// should only get here if the entire buffer had no newlines
		// or the very last char in the buffer was '\r'.
		return NewLineMode.Standard;
	}

	/// <summary>
	/// Initializes the schema starting with the current row.
	/// </summary>
	public void Initialize()
	{
		var count = schema?.GetFieldCount(this) ?? this.curFieldCount;
		columns = new CsvColumn[count];
		this.fieldCount = count;
		for (int i = 0; i < count; i++)
		{
			var name = hasHeaders ? GetStringRaw(i) : null;
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
		this.colCache = new OrdinalCache[this.fieldCount];
		this.state = hasHeaders ? State.Open : State.Initialized;
	}

#if INTRINSICS

	Vector256<byte> delimiterMaskVector256;
	Vector256<byte> lineEndMaskVector256;
	Vector256<byte> quoteMaskVector256;
	Vector128<byte> delimiterMaskVector;
	Vector128<byte> lineEndMaskVector;
	Vector128<byte> quoteMaskVector;

	void InitIntrinsics()
	{
		delimiterMaskVector256 = Vector256.Create((byte)this.delimiter);
		lineEndMaskVector256 = Vector256.Create((byte)(newLineMode == NewLineMode.MacOS ? '\r' : '\n'));
		quoteMaskVector256 = Vector256.Create((byte)this.quote);
		delimiterMaskVector = Vector128.Create((byte)this.delimiter);
		lineEndMaskVector = Vector128.Create((byte)(newLineMode == NewLineMode.MacOS ? '\r' : '\n'));
		quoteMaskVector = Vector128.Create((byte)this.quote);
	}

	// this method uses SIMD instructions to optimistically read records
	// as fast as possible. To keep things simple, this method stops processing
	// when quotes are detected and falls back to the slower, more robust single-data
	// path. Returns true when the end-of-record newline is found, and false when
	// the record might contain more fields.
	bool ReadRecordFast(ref int fieldIdx)
	{
		if (this.style != CsvStyle.Standard)
		{
			return false;
		}

		if (Bmi1.IsSupported)
		{
			if (Avx2.IsSupported)
			{
				return ReadRecordFast256(ref fieldIdx);
			}
			else
			if (Sse2.IsSupported)
			{
				return ReadRecordFast128(ref fieldIdx);
			}
		}
		return false;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	unsafe bool ReadRecordFast256(ref int fieldIdx)
	{
		var pos = idx;
		// processing 32-char chunks so terminate the
		// fast loop before we run out of data.
		var end = this.bufferEnd - 32;

		var recordStart = this.recordStart;
		var fieldInfos = this.fieldInfos;

		fixed (char* p = buffer)
		{
			// SIMD operators support ushort.
			short* ip = (short*)p;
			while (pos < end)
			{
				// a single 16-byte block filled with delimiters can result in 16 new fields
				// being required, so make sure we have room for that case ahead of time.
				if (fieldIdx + 32 >= fieldInfos.Length)
				{
					Array.Resize(ref fieldInfos, fieldInfos.Length + 32);
					this.fieldInfos = fieldInfos;
				}

				var v1 = Avx2.LoadVector256(ip + pos);
				var v2 = Avx2.LoadVector256(ip + pos + 16);

				var dataVector = Avx2.PackUnsignedSaturate(v1, v2);
				dataVector = Avx2.Permute4x64(dataVector.AsInt64(), 0b11_01_10_00).AsByte();

				// produce vectors indicating where delimiters, line ends '\n' and quotes are.
				// these vectors will contain 16 byte elements that are either 0 or byte.MaxValue
				// indicating where the characters are detected.
				var delimiterVector = Avx2.CompareEqual(dataVector, delimiterMaskVector256);
				var lineEndVector = Avx2.CompareEqual(dataVector, lineEndMaskVector256);
				var quoteVector = Avx2.CompareEqual(dataVector, quoteMaskVector256);

				var delimiterMask = (uint)Avx2.MoveMask(delimiterVector);

				// combine the line end and quote vectors to
				// check if we need to handle those with a single branch
				var combined = Avx2.Or(lineEndVector, quoteVector);
				if (Avx2.MoveMask(combined.AsByte()) != 0)
				{
					var lineEndMask = (uint)Avx2.MoveMask(lineEndVector);
					var quoteMask = (uint)Avx2.MoveMask(quoteVector);

					// find the locations of the first line end and quotes
					var lineEndIdx = (int)Bmi1.TrailingZeroCount(lineEndMask);
					var quoteIdx = (int)Bmi1.TrailingZeroCount(quoteMask);

					// if the first quote appears before the first EOL
					// abort the SIMD path and let the single-data path process it
					if (quoteIdx < lineEndIdx)
					{
						this.curFieldCount = fieldIdx;
						return false;
					}

					// if a lineEnd is present in this block
					if (lineEndIdx < 0x20)
					{
						// process any delimiters that appear before the line end
						while (delimiterMask != 0)
						{
							var delimiterIdx = (int)Bmi1.TrailingZeroCount(delimiterMask);
							if (delimiterIdx >= lineEndIdx)
							{
								break;
							}

							var endIdx = pos + delimiterIdx;
							SetFieldEnd(ref fieldIdx, endIdx);
							delimiterMask = Bmi1.ResetLowestSetBit(delimiterMask);
						}

						// then process the line ending
						{
							var endIdx = pos + lineEndIdx;
							ref var field = ref SetFieldEnd(ref fieldIdx, endIdx);
							// might need to also remove a preceding '\r'
							if (newLineMode != NewLineMode.MacOS && p[endIdx - 1] == '\r')
							{
								field.endIdx--;
							}
						}
						this.curFieldCount = fieldIdx;
						return true;
					}
				}

				// process any delimiters in this block.
				while (delimiterMask != 0)
				{
					var delimiterIdx = (int)Bmi1.TrailingZeroCount(delimiterMask);
					int endIdx = pos + delimiterIdx;
					SetFieldEnd(ref fieldIdx, endIdx);
					delimiterMask = Bmi1.ResetLowestSetBit(delimiterMask);
				}

				pos += 32;
			}

			this.curFieldCount = fieldIdx;
			return false;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	unsafe bool ReadRecordFast128(ref int fieldIdx)
	{
		var pos = idx;
		// processing 16-char chunks so terminate the
		// fast loop before we run out of data.
		var end = this.bufferEnd - 16;

		var recordStart = this.recordStart;
		var fieldInfos = this.fieldInfos;

		fixed (char* p = buffer)
		{
			// SIMD operators support ushort.
			short* ip = (short*)p;
			while (pos < end)
			{
				// a single 16-byte block filled with delimiters can result in 16 new fields
				// being required, so make sure we have room for that case ahead of time.
				if (fieldIdx + 16 >= fieldInfos.Length)
				{
					Array.Resize(ref fieldInfos, fieldInfos.Length + 16);
					this.fieldInfos = fieldInfos;
				}

				// load a vector with 16 bytes where each byte represents
				// a single character. Chars that are above 255 will "saturate"
				// to 255.
				var v1 = Sse2.LoadVector128(ip + pos);
				var v2 = Sse2.LoadVector128(ip + pos + 8);

				var dataVector = Sse2.PackUnsignedSaturate(v1, v2);

				// produce vectors indicating where delimiters, line ends '\n' and quotes are.
				// these vectors will contain 16 byte elements that are either 0 or byte.MaxValue
				// indicating where the characters are detected.
				var delimiterVector = Sse2.CompareEqual(dataVector, delimiterMaskVector);
				var lineEndVector = Sse2.CompareEqual(dataVector, lineEndMaskVector);
				var quoteVector = Sse2.CompareEqual(dataVector, quoteMaskVector);

				var delimiterMask = (uint)Sse2.MoveMask(delimiterVector.AsByte());

				// combine the line end and quote vectors to
				// check if we need to handle those with a single branch
				var combined = Sse2.Or(lineEndVector, quoteVector);
				if (Sse2.MoveMask(combined.AsByte()) != 0)
				{
					var lineEndMask = (uint)Sse2.MoveMask(lineEndVector.AsByte());
					var quoteMask = (uint)Sse2.MoveMask(quoteVector.AsByte());

					// find the locations of the first line end and quotes
					var lineEndIdx = (int)Bmi1.TrailingZeroCount(lineEndMask);
					var quoteIdx = (int)Bmi1.TrailingZeroCount(quoteMask);

					// if the first quote appears before the first EOL
					// abort the SIMD path and let the single-data path process it
					if (quoteIdx < lineEndIdx)
					{
						this.curFieldCount = fieldIdx;
						return false;
					}

					// if a lineEnd is present in this block
					if (lineEndIdx < 0x20)
					{
						// process any delimiters that appear before the line end
						while (delimiterMask != 0)
						{
							var delimiterIdx = (int)Bmi1.TrailingZeroCount(delimiterMask);
							if (delimiterIdx >= lineEndIdx)
							{
								break;
							}

							var endIdx = pos + delimiterIdx;
							SetFieldEnd(ref fieldIdx, endIdx);
							delimiterMask = Bmi1.ResetLowestSetBit(delimiterMask);
						}

						// then process the line ending
						{
							var endIdx = pos + lineEndIdx;
							ref var field = ref SetFieldEnd(ref fieldIdx, endIdx);
							// might need to also remove a preceding '\r'
							if (newLineMode != NewLineMode.MacOS && p[endIdx - 1] == '\r')
							{
								field.endIdx--;
							}
						}
						this.curFieldCount = fieldIdx;
						return true;
					}
				}

				// process any delimiters in this block.
				while (delimiterMask != 0)
				{
					var delimiterIdx = (int)Bmi1.TrailingZeroCount(delimiterMask);
					int endIdx = pos + delimiterIdx;
					SetFieldEnd(ref fieldIdx, endIdx);
					delimiterMask = Bmi1.ResetLowestSetBit(delimiterMask);
				}

				pos += 16;
			}

			this.curFieldCount = fieldIdx;
			return false;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	ref FieldInfo SetFieldEnd(ref int fieldIdx, int endIdx)
	{
		this.idx = endIdx + 1;
		ref var field = ref fieldInfos[fieldIdx++];
		field = default;
		field.endIdx = endIdx - recordStart;
		return ref field;
	}

#endif

	bool GrowBuffer()
	{
		var len = buffer.Length;
		if (maxBufferSize > len)
		{
			var newLen = Math.Min(len * 2, maxBufferSize);
			var newBuffer = new char[newLen];
			Array.Copy(buffer, recordStart, newBuffer, 0, bufferEnd - recordStart);
			this.idx -= recordStart;
			this.bufferEnd -= recordStart;
			this.recordStart = 0;

			this.buffer = newBuffer;
			return true;
		}
		return false;
	}

	// attempt to read a field. 
	// returns True if there are more in record (hit delimiter), 
	// False if last in record (hit eol/eof), 
	// or Incomplete if we exhausted the buffer before finding the end of the record.
	ReadResult ReadField(int fieldIdx)
	{
		char c;
		var idx = this.idx;
		var buffer = this.buffer;

		// this will remain -1 if it is unquoted. 
		// Otherwise we use it to determine if the quotes were "clean".
		var closeQuoteIdx = -1;
		int escapeCount = 0;
		int fieldEnd = 0;
		bool last = false;
		bool complete = false;

		if (fieldIdx >= fieldInfos.Length)
		{
			// this resize is constrained by the fact that the record has to fit in one row
			Array.Resize(ref fieldInfos, fieldInfos.Length * 2);
		}
		ref var fi = ref fieldInfos[fieldIdx];

		if (style == CsvStyle.Escaped)
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
							idx--;// "unconsume" the newline character, so that ConsumeLineEnd can process it.
								  // if the escape precede an EOL, we might have to consume 2 chars
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
							// there was nothing to escape
							pendingException = new CsvFormatException(rowNumber, fieldIdx);
							return ReadResult.False;
						}
						return ReadResult.Incomplete;
					}
				}
				if (c == delimiter || IsEndOfLine(c))
				{
					// "unread" the delimiter/eol, and let the normal code path handle it
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
					idx++; // consume the quote we just read
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
									fieldEnd = closeQuoteIdx;
									// the quote (escape) we just saw was a the closing quote
									break;
								}
							}
							else
							{
								if (atEndOfText)
								{
									if (escape == quote)
									{
										complete = true;
										last = true;
										closeQuoteIdx = idx;
										fieldEnd = closeQuoteIdx;
										// the quote (escape) we just saw was a the closing quote
									}
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
							fieldEnd = closeQuoteIdx;
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
			if (c <= minSafe)
			{
				if (c == delimiter)
				{
					this.idx = idx;
					fieldEnd = idx - 1 - recordStart;
					complete = true;
					break;
				}
				else
				// this handles the case where we had a quoted field
				if (c == quote && closeQuoteIdx >= 0)
				{
					if (style == CsvStyle.Lax)
					{
						fi.quoteState = QuoteState.InvalidQuotes;
					}
					else
					{
						this.pendingException = new CsvFormatException(rowNumber, fieldIdx);
						return ReadResult.False;
					}
				}
				else
				if (IsEndOfLine(c))
				{
					idx--;
					var temp = idx;
					var r = ConsumeLineEnd(buffer, ref idx);
					if (r == ReadResult.Incomplete)
					{
						return ReadResult.Incomplete;
					}
					if (r == ReadResult.False)
					{
						continue;
					}
					fieldEnd = temp - recordStart;
					complete = true;
					last = true;
					break;
				}
			}
			else
			{
				if (closeQuoteIdx >= 0)
				{
					if (style == CsvStyle.Lax)
					{
						// in lax mode, we'll continue reading the remainder of the field
						// after the closig quote
						fi.quoteState = QuoteState.InvalidQuotes;
					}
					else
					{
						// if the field is quoted, we shouldn't be here.
						// the only valid characters would be a delimiter, a new line, or EOF.
						this.pendingException = new CsvFormatException(rowNumber, fieldIdx);
						return ReadResult.False;
					}
				}
			}
		}

		if (complete || atEndOfText)
		{

			if (atEndOfText && !complete)
			{
				fieldEnd = idx;
			}

			curFieldCount++;


			if (style == CsvStyle.Escaped)
			{
				fi.quoteState =
					escapeCount == 0
					? QuoteState.Unquoted
					: QuoteState.ImplicitQuotes;
			}
			else
			{
				if (closeQuoteIdx == -1)
				{
					fi.quoteState = QuoteState.Unquoted;
				}
				else
				{
					if (fieldEnd == (closeQuoteIdx - recordStart))
					{
						fi.quoteState = QuoteState.Quoted;
					}
					else
					{
						fi.quoteState = QuoteState.InvalidQuotes;
						if (style != CsvStyle.Lax)
						{
							var rowNumber = this.rowNumber == 0 && this.state == State.Initialized ? 1 : this.rowNumber;
							this.pendingException = new CsvFormatException(rowNumber, fieldIdx);
							return ReadResult.False;
						}
					}
				}
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

	ReadResult ReadComment(char[] buffer, ref int idx)
	{
		// only called in a context where we're definitely not
		// at the end of the buffer.
		var end = bufferEnd;
		var c = buffer[idx];
		if (c == comment)
		{
			int i = idx;
			for (; i < end; i++)
			{
				c = buffer[i];
				if (IsEndOfLine(c))
				{
					var e = i;
					var r = ConsumeLineEnd(buffer, ref i);
					if (r == ReadResult.Incomplete)
					{
						return r;
					}
					if (r == ReadResult.False)
					{
						continue;
					}
					if (this.commentHandler != null)
					{
						var s = idx + 1;
						var str = new string(buffer, s, e - s);
						this.commentHandler.HandleComment(this, str);
					}
					idx = i;
					this.recordStart = idx;
					return ReadResult.True;
				}
			}
			if (atEndOfText)
			{
				idx = i;
				this.recordStart = idx;
				return ReadResult.True;
			}
			return ReadResult.Incomplete;
		}
		return ReadResult.False;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static bool IsEndOfLine(char c)
	{
		return c <= '\r' && (c == '\n' || c == '\r');
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
					idx--;
					return ReadResult.True;
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
		// this can never be reached, as this is only called
		// in a context where c is '\r' or '\n'
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
		ValidateState();
		// four cases:
		// true and false both not null. Any other value raises error.
		// true not null, false null. True string true, anything else false.
		// false not null, true null. False string false, anything else true.
		// both null: attempt to parse a bool then attempt to parse as int
		var col = this.columns[ordinal];
		var trueString = col.TrueString ?? this.trueString;
		var falseString = col.FalseString ?? this.falseString;
#if SPAN
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
		ValidateState();
#if SPAN
		return byte.Parse(this.GetFieldSpan(ordinal), provider: culture);
#else
		return byte.Parse(this.GetString(ordinal), culture);
#endif
	}

	/// <inheritdoc/>
	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		ValidateState();
		if (buffer == null)
		{
			return GetBinaryLength(ordinal);
		}
		if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));

		var col = this.columns[ordinal];
		var encoding = col.ColumnBinaryEncoding ?? this.binaryEncoding;

		return encoding switch
		{
			BinaryEncoding.Base64 => GetBytesBase64(ordinal, (int)dataOffset, buffer, bufferOffset, length),
			BinaryEncoding.Hexadecimal => (long)GetBytesHex(ordinal, (int)dataOffset, buffer, bufferOffset, length),
			_ => throw new NotSupportedException(),// TODO: improve error message.
		};
	}

	int GetBytesBase64(int ordinal, int dataOffset, byte[] buffer, int bufferOffset, int length)
	{
		scratch ??= new byte[3];

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
			CsvDataReader.FromBase64Chars(iBuf, o + iOff, 4, scratch, 0, out int c);
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
				CsvDataReader.FromBase64Chars(iBuf, o + iOff, charCount, oBuf, oOff, out int c);
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
			CsvDataReader.FromBase64Chars(iBuf, o + iOff, 4, scratch, 0, out int c);
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

		var outLen = GetHexLength(cs, out bool hasPrefix);
		if (hasPrefix)
		{
			o += 2;
		}

		var c = Math.Min(outLen - dataOffset, length);

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

	const byte Invalid = 255;

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

	static byte HexValue(char c)
	{
		if (c > 128) return Invalid;
		return HexMap[c];
	}

	static void FromBase64Chars(char[] chars, int charsOffset, int charsLen, byte[] bytes, int bytesOffset, out int bytesWritten)
	{
#if SPAN
		if (!Convert.TryFromBase64Chars(chars.AsSpan().Slice(charsOffset, charsLen), bytes.AsSpan(bytesOffset), out bytesWritten))
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
		ValidateState();
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
		ValidateState();
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

	/// <summary>
	/// Gets the value of the field as a <see cref="TimeSpan"/>.
	/// </summary>
	public TimeSpan GetTimeSpan(int ordinal)
	{
		ValidateState();
		var format = columns[ordinal].Format;
#if SPAN
		var span = this.GetFieldSpan(ordinal);
		if (format != null && TimeSpan.TryParseExact(span, format, culture, TimeSpanStyles.None, out var value))
		{
			return value;
		}
		return TimeSpan.Parse(span, culture);
#else
		var str = this.GetString(ordinal);
		if (format != null && TimeSpan.TryParseExact(str, format, culture, TimeSpanStyles.None, out var value))
		{
			return value;
		}
		return TimeSpan.Parse(str, culture);
#endif
	}

	/// <summary>
	/// Gets the value of the field as a <see cref="DateTimeOffset"/>.
	/// </summary>
	public DateTimeOffset GetDateTimeOffset(int ordinal)
	{
		ValidateState();
		var format = columns[ordinal].Format ?? this.dateTimeFormat;
		DateTimeOffset value;
#if SPAN
		var span = this.GetFieldSpan(ordinal);

		if (IsoDate.TryParse(span, out value))
			return value;

		if (format != null && DateTimeOffset.TryParseExact(span, format, culture, DateTimeStyles.RoundtripKind, out value))
		{
			return value;
		}
		return DateTimeOffset.Parse(span, culture, DateTimeStyles.RoundtripKind);
#else
		var str = this.GetString(ordinal);
		if (format != null && DateTimeOffset.TryParseExact(str, format, culture, DateTimeStyles.RoundtripKind, out value))
		{
			return value;
		}
		return DateTimeOffset.Parse(str, culture, DateTimeStyles.RoundtripKind);
#endif
	}

	/// <inheritdoc/>
	public override DateTime GetDateTime(int ordinal)
	{
		ValidateState();
		DateTime value;
#if SPAN
		var span = this.GetFieldSpan(ordinal);
		if (IsoDate.TryParse(span, out value))
			return value;

		var format = columns[ordinal].Format ?? this.dateTimeFormat;
		if (format != null && DateTime.TryParseExact(span, format, culture, DateTimeStyles.RoundtripKind, out value))
		{
			return value;
		}
		return DateTime.Parse(span, culture, DateTimeStyles.RoundtripKind);
#else
		var dateStr = this.GetString(ordinal);

		var format = columns[ordinal].Format ?? this.dateTimeFormat ?? "O";
		if (format != null && DateTime.TryParseExact(dateStr, format, culture, DateTimeStyles.RoundtripKind, out value))
		{
			return value;
		}
		return DateTime.Parse(dateStr, culture, DateTimeStyles.RoundtripKind);
#endif
	}

	/// <inheritdoc/>
	public override decimal GetDecimal(int ordinal)
	{
		ValidateState();
#if SPAN
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
		ValidateState();
#if SPAN
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
		ValidateState();
#if SPAN
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
		ValidateState();
#if SPAN
		return Guid.Parse(this.GetFieldSpan(ordinal));
#else
		return Guid.Parse(this.GetString(ordinal));
#endif
	}

	/// <inheritdoc/>
	public override short GetInt16(int ordinal)
	{
		ValidateState();
#if SPAN
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
		ValidateState();
#if SPAN
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
		ValidateState();
#if SPAN
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

	[MethodImpl(MethodImplOptions.NoInlining)]
	int LookupOrdinal(string name)
	{
		var columnIndex = this.headerMap.TryGetValue(name, out var idx) ? idx : throw new IndexOutOfRangeException();
		if (columnIndex == -1)
			throw new AmbiguousColumnException(name);
		return idx;
	}

	/// <inheritdoc/>
	public override int GetOrdinal(string name)
	{
		if (name == null) throw new ArgumentNullException(nameof(name));

		if (colCacheIdx < colCache.Length)
		{
			ref var col = ref colCache[colCacheIdx];
			colCacheIdx++;
			if (!ReferenceEquals(name, col.name))
			{
				col = new OrdinalCache { name = name, idx = LookupOrdinal(name) };
			}
			return col.idx;
		}
		return LookupOrdinal(name);
	}

	/// <inheritdoc/>
	public override string GetString(int ordinal)
	{
		ValidateState();
		return GetStringRaw(ordinal);
	}

	string GetStringRaw(int ordinal)
	{
		if ((uint)ordinal < (uint)curFieldCount)
		{
			var s = GetFieldUnsafe(ordinal);
			var l = s.length;
			if (l == 0) return string.Empty;
			return stringFactory.Invoke(this, ordinal, s.buffer, s.offset, l) ?? new string(s.buffer, s.offset, l);
		}
		if ((uint)ordinal >= (uint)fieldCount)
		{
			throw new ArgumentOutOfRangeException(nameof(ordinal));
		}
		return string.Empty;
	}

	internal readonly struct CharSpan
	{
		internal readonly static CharSpan Empty = new(Array.Empty<char>(), 0, 0);

		public readonly char[] buffer;
		public readonly int offset;
		public readonly int length;

		public CharSpan(char[] buffer, int offset, int length)
		{
#if DEBUG
			if (offset < 0 || length < 0)
			{
				throw new Exception();
			}
#endif
			Debug.Assert(offset >= 0);
			Debug.Assert(length >= 0);
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

#if SPAN

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal Span<char> ToSpan()
		{
			var span = buffer.AsSpan().Slice(offset, length);
			return span;
		}

#endif

#if DEBUG
		public override string ToString()
		{
			return new string(buffer, offset, length);
		}
#endif
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal CharSpan GetField(int ordinal)
	{
		if ((uint)ordinal < (uint)this.curFieldCount)
		{
			return GetFieldUnsafe(ordinal);
		}

		return CharSpan.Empty;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	CharSpan GetFieldUnsafe(int ordinal)
	{
		// "Unsafe" meaning this should only be called
		// in contexts where ordinal is already validated to be in-range

		ref var fi = ref this.fieldInfos[ordinal];
		var startIdx = recordStart + (ordinal == 0 ? 0 : this.fieldInfos[ordinal - 1].endIdx + 1);
		var endIdx = recordStart + fi.endIdx;
		int offset = startIdx;
		int len = endIdx - startIdx;
		var buffer = this.buffer;
		if (fi.quoteState != QuoteState.Unquoted)
		{
			switch (fi.quoteState)
			{
				case QuoteState.InvalidQuotes:
					return PrepareInvalidField(offset, len);
				case QuoteState.Quoted:
					// trim the quotes
					offset += 1;
					len -= 2;
					if (fi.escapeCount > 0)
					{
						goto case QuoteState.ImplicitQuotes;
					}
					break;
				case QuoteState.ImplicitQuotes: // escaped
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
		if (scratchStr.Length < len)
		{
			// otherwise we'll allocate a buffer
			scratchStr = new char[len];
		}

		int i = 0;
		int d = 0;
		while (i < len)
		{
			var c = buffer[offset + i++];
			if (inQuote)
			{
				if (c == escape)
				{
					if (i < len)
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
					{
						// we should never get here. Invalid fields should always be
						// handled in ReadField and end up in PrepareInvalidField
						throw new CsvFormatException(rowNumber, -1);
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
			scratchStr[d++] = c;
		}
		return new CharSpan(scratchStr, 0, eLen);
	}

	char[] scratchStr = Array.Empty<char>();

	// this should only be called in Lax mode, otherwise an exception
	// would have been thrown in ReadField.
	CharSpan PrepareInvalidField(int offset, int len)
	{
		bool inQuote = false;

		// increase the scratch space if needed.
		if (scratchStr.Length < len)
		{
			scratchStr = new char[len];
		}

		int i = 0;
		if (buffer[offset + i] == quote)
		{
			i++;
			inQuote = true;
		}

		int d = 0;
		while (i < len)
		{
			var c = buffer[offset + i++];
			if (inQuote)
			{
				if (c == escape)
				{
					if (i < len)
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
			scratchStr[d++] = c;
		}
		return new CharSpan(scratchStr, 0, d);
	}

	/// <inheritdoc/>
	public override object GetValue(int ordinal)
	{
		ValidateState();
		var max = Math.Max(this.fieldCount, this.curFieldCount);

		if (ordinal > max)
		{
			throw new ArgumentOutOfRangeException(nameof(ordinal));
		}

		var col = ordinal < columns.Length ? columns[ordinal] : null;

		if (col?.AllowDBNull != false && this.IsDBNull(ordinal))
		{
			return DBNull.Value;
		}

		IFieldAccessor acc = col?.Accessor ?? StringAccessor.Instance;
		return acc.GetValueAsObject(this, ordinal);
	}

	int GetBinaryLength(int ordinal)
	{
		var span = this.GetField(ordinal);
		var col = this.columns[ordinal];

		var enc = col.ColumnBinaryEncoding ?? this.binaryEncoding;
		return enc switch
		{
			BinaryEncoding.Base64 => GetBase64Length(span),
			BinaryEncoding.Hexadecimal => GetHexLength(span, out _),
			_ => throw new NotSupportedException(),// TODO: improve error message.
		};
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
		// must be divisible by 2
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
	public override int GetValues(object[] values)
	{
		ValidateState();
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
		ValidateState();
		if (ordinal < 0) throw new ArgumentOutOfRangeException(nameof(ordinal));

		if ((uint)ordinal < this.fieldCount)
		{
			var col = columns[ordinal];

			if (col.AllowDBNull == false)
			{
				// if the schema claims it is not nullable then we will honor that claim
				// if the schema is wrong a FormatException or CastException may result
				// when trying to access the value.
				return false;
			}
		}

		if ((uint)ordinal >= (uint)curFieldCount)
		{
			// if the current row has missing fields, consider them null
			return true;
		}

		// now pay the cost of determining if the thing is null.
		var infos = fieldInfos;
		ref var fi = ref infos[ordinal];

		var startIdx = recordStart + (ordinal == 0 ? 0 : infos[ordinal - 1].endIdx + 1);
		var endIdx = recordStart + fi.endIdx;

		if (fi.quoteState == QuoteState.Quoted)
		{
			var col = ordinal < this.fieldCount ? columns[ordinal] : null;
			if (col?.DataType != typeof(string))
			{
				var len = endIdx - startIdx;
				// 2 characters, empty quotes.
				return len == 2;
			}
		}

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

		public IFieldAccessor Accessor { get; }

		public CsvColumn(string? name, int ordinal, DbColumn? schema = null)
		{
			// non-overridable
			this.ColumnOrdinal = ordinal;
			this.IsReadOnly = true; // I don't understand what false would mean here.

			var colName = schema?.ColumnName;

			this.ColumnName = string.IsNullOrEmpty(colName) ? name ?? "" : colName;
			this.DataType = schema?.DataType ?? typeof(string);
			this.DataTypeName = schema?.DataTypeName ?? this.DataType.Name;

			this.Accessor = CsvDataAccessor.GetAccessor(this.DataType) ?? StringAccessor.Instance;

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
			this.BaseColumnName = schema?.BaseColumnName ?? name; // default in the original header name if they chose to remap it.
			this.BaseCatalogName = schema?.BaseCatalogName;
			this.UdtAssemblyQualifiedName = schema?.UdtAssemblyQualifiedName;
			this.Format = schema?[nameof(Format)] as string;
			if (this.DataType == typeof(byte[]))
			{
				this.ColumnBinaryEncoding = CsvColumn.GetBinaryEncoding(this.Format);
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

		static BinaryEncoding? GetBinaryEncoding(string? format)
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
				return property switch
				{
					nameof(Format) => Format,
					_ => base[property],
				};
			}
		}
	}

#if SPAN

	/// <summary>
	/// Gets a span containing the characters of a field.
	/// </summary>
	/// <remarks> The contents of the returned span will have any quotes removed and be fully unescaped. </remarks>
	/// <param name="ordinal">The field ordinal.</param>
	/// <returns>A span containing the characters of the field.</returns>
	public ReadOnlySpan<char> GetFieldSpan(int ordinal)
	{
		ValidateState();
		var s = GetField(ordinal);
		return s.ToSpan();
	}

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

	static class Accessor<T>
	{
		public static IFieldAccessor<T> Instance;

		static Accessor()
		{
			Instance = GetAccessor();
		}

		static IFieldAccessor<T> GetAccessor()
		{
			if (CsvDataAccessor.Instance is not IFieldAccessor<T> acc)
			{
				if (typeof(T).IsEnum)
				{
					return EnumAccessor<T>.Instance;
				}
				throw new NotSupportedException(); // TODO: exception type?
			}
			return acc;
		}
	}

	/// <inheritdoc/>
	public override T GetFieldValue<T>(int ordinal)
	{
		ValidateState();
		var acc = Accessor<T>.Instance;
		return acc.GetValue(this, ordinal);
	}
}
