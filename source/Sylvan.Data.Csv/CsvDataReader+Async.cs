using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv;

partial class CsvDataReader
{
	/// <summary>
	/// Creates a new CsvDataReader asynchronously.                                                                                          
	/// </summary>
	/// <param name="filename">The name of a file containing CSV data.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
	public static Task<CsvDataReader> CreateAsync(string filename, CsvDataReaderOptions? options)
	{
		return CreateAsync(filename, options, default);
	}

	/// <summary>
	/// Creates a new CsvDataReader asynchronously.                                                                                          
	/// </summary>
	/// <param name="filename">The name of a file containing CSV data.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <param name="cancel">The optional cancellation token to be used for cancelling the initialization sequence.</param>
	/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
	public static Task<CsvDataReader> CreateAsync(string filename, CsvDataReaderOptions? options = null, CancellationToken cancel = default)
	{
		if (filename == null) throw new ArgumentNullException(nameof(filename));
		// TextReader must be owned when we open it.
		if (options?.OwnsReader == false) throw new CsvConfigurationException();
		var bufferSize = GetIOBufferSize(options);
		var stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);
		var reader = new StreamReader(stream, Encoding.Default);
		return CreateAsyncInternal(reader, null, options, cancel);
	}

	/// <summary>
	/// Creates a new CsvDataReader asynchronously.
	/// </summary>
	/// <param name="reader">The TextReader for the delimited data.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
	public static Task<CsvDataReader> CreateAsync(TextReader reader, CsvDataReaderOptions? options)
	{
		return CreateAsyncInternal(reader, null, options, default);
	}

	/// <summary>
	/// Creates a new CsvDataReader asynchronously.
	/// </summary>
	/// <param name="reader">The TextReader for the delimited data.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <param name="cancel">The optional cancellation token to be used for cancelling the initialization sequence.</param>
	/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
	public static Task<CsvDataReader> CreateAsync(TextReader reader, CsvDataReaderOptions? options = null, CancellationToken cancel = default)
	{
		return CreateAsyncInternal(reader, null, options, cancel);
	}

	/// <summary>
	/// Creates a new CsvDataReader asynchronously.
	/// </summary>
	/// <param name="reader">The TextReader for the delimited data.</param>
	/// <param name="buffer">A buffer to use for internal processing.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
	public static Task<CsvDataReader> CreateAsync(TextReader reader, char[] buffer, CsvDataReaderOptions? options)
	{
		return CreateAsyncInternal(reader, buffer, options, default);
	}

	/// <summary>
	/// Creates a new CsvDataReader asynchronously.
	/// </summary>
	/// <param name="reader">The TextReader for the delimited data.</param>
	/// <param name="buffer">A buffer to use for internal processing.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <param name="cancel">The optional cancellation token to be used for cancelling the initialization sequence.</param>
	/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
	public static Task<CsvDataReader> CreateAsync(TextReader reader, char[] buffer, CsvDataReaderOptions? options = null, CancellationToken cancel = default)
	{
		return CreateAsyncInternal(reader, buffer, options, cancel);
	}

	static async Task<CsvDataReader> CreateAsyncInternal(TextReader reader, char[]? buffer, CsvDataReaderOptions? options, CancellationToken cancel)
	{
		options ??= CsvDataReaderOptions.Default;
		if (reader == null) throw new ArgumentNullException(nameof(reader));
		var csv = new CsvDataReader(reader, buffer, options);
		if (!await csv.InitializeReaderAsync(cancel).ConfigureAwait(false) && options.HasHeaders)
		{
			throw new CsvMissingHeadersException();
		}
		return csv;
	}

	static bool ShouldSkip(char[] buffer, int offset, int length)
	{
		if (length == 0) return true;
		for (int i = 0; i < length; i++)
		{
			var c = buffer[offset + i];
			if (!char.IsWhiteSpace(c))
			{
				return false;
			}
		}
		return false;
	}

	async Task<bool> InitializeReaderAsync(CancellationToken cancel = default)
	{
		await FillBufferAsync(cancel).ConfigureAwait(false);

		bool skip = resultSetMode == ResultSetMode.MultiResult;
		while (skip)
		{
			skip = false;
			if (bufferEnd == 0) break;
			for (int i = idx; i < bufferEnd; i++)
			{
				var c = buffer[i];
				if (IsEndOfLine(c))
				{
					if (ShouldSkip(buffer, idx, i - idx))
					{
						skip = true;
						idx = i + 1;
					}
					break;
				}
			}
		}

		if (autoDetectDelimiter)
		{
			var c = DetectDelimiter();
			this.delimiter = c;
		}
		if (this.newLineMode == NewLineMode.Unknown)
		{
			this.newLineMode = this.DetectNewLine();
		}

		this.minSafe = delimiter < '\r' ? '\r' : delimiter;
		this.minSafe = minSafe > quote ? minSafe : quote;

#if INTRINSICS
		InitIntrinsics();
#endif

		// if the user specified that there are headers
		// read them, and use them to determine fieldCount.
		if (hasHeaders)
		{
			if (carryRow || state == State.Open || await NextRecordAsync(cancel).ConfigureAwait(false))
			{
				ThrowPendingException();

				carryRow = false;
				Initialize();
				this.state = State.Initialized;
				this.rowNumber++;
				var hasNext = await NextRecordAsync(cancel).ConfigureAwait(false);
				if (resultSetMode == ResultSetMode.SingleResult)
				{
					this.hasRows = hasNext;
				}
				else
				{
					this.hasRows = hasNext && this.fieldCount == this.curFieldCount;
					this.state = State.Initialized;
					this.carryRow = this.hasRows == false && this.fieldCount > 0;
				}
			}
			else
			{
				return false;
			}
		}
		else
		{
			// read the first row of data to determine fieldCount (if there were no headers)
			// and support calling HasRows before Read is first called.
			this.hasRows = carryRow || await NextRecordAsync(cancel).ConfigureAwait(false);
			this.carryRow = false;
			Initialize();
			return fieldCount != 0;
		}
		this.rowNumber = 0;

		return true;
	}

	async Task<bool> NextRecordAsync(CancellationToken cancel = default)
	{
		this.curFieldCount = 0;
		this.recordStart = this.idx;
	start:
		if (this.idx >= bufferEnd)
		{
			await FillBufferAsync(cancel).ConfigureAwait(false);
			if (idx == bufferEnd)
			{
				return false;
			}
		}

		var result = ReadComment(buffer, ref this.idx);
		if (result != ReadResult.False)
		{
			if (result == ReadResult.Incomplete)
			{
				if (recordStart == 0 && this.bufferEnd == this.buffer.Length)
				{
					if (!GrowBuffer())
					{
						ThrowRecordTooLarge();
					}
				}
				await FillBufferAsync(cancel).ConfigureAwait(false);
			}
			goto start;
		}

		int fieldIdx = 0;
		while (true)
		{
#if INTRINSICS

			if (ReadRecordFast(ref fieldIdx))
			{
				return true;
			}

#endif
			result = ReadField(fieldIdx);

			if (result == ReadResult.True)
			{
				fieldIdx++;
				continue;
			}
			if (result == ReadResult.False)
			{
				if (this.state == State.Open)
				{
					ThrowPendingException();
				}
				return true;
			}

			// we were unable to read an entire record out of the buffer
			if (recordStart == 0 && bufferEnd == buffer.Length)
			{
				if (!GrowBuffer())
				{
					ThrowRecordTooLarge(fieldIdx);
				}
			}
			await FillBufferAsync(cancel).ConfigureAwait(false);
			// after filling the buffer, we will resume reading fields from where we left off.
		}
	}

	async Task<int> FillBufferAsync(CancellationToken cancel = default)
	{
		var buffer = this.buffer;
		if (recordStart != 0)
		{
			// move any pending data to the front of the buffer.
			Array.Copy(buffer, recordStart, buffer, 0, bufferEnd - recordStart);
		}

		bufferEnd -= recordStart;
		idx -= recordStart;
		recordStart = 0;

		var count = buffer.Length - bufferEnd;
#if SPAN
		var memory = new Memory<char>(buffer, bufferEnd, count);
		var c = await reader.ReadAsync(memory, cancel).ConfigureAwait(false);
#else
		var c = await reader.ReadAsync(buffer, bufferEnd, count).ConfigureAwait(false);
#endif
		bufferEnd += c;
		if (c == 0)
		{
			atEndOfText = true;
		}
		return c;
	}

	/// <inheritdoc/>
	public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		this.colCacheIdx = 0;
		if (this.state == State.Open)
		{
			this.rowNumber++;
			var success = await this.NextRecordAsync(cancellationToken).ConfigureAwait(false);
			return ReadFinish(success);
		}
		return ReadCommon();
	}

	/// <inheritdoc/>
	public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
	{
		while (await ReadAsync(cancellationToken).ConfigureAwait(false)) ;
		return await InitializeReaderAsync(cancellationToken).ConfigureAwait(false);
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
