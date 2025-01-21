using System;
using System.IO;
using System.Text;

namespace Sylvan.Data.Csv;

partial class CsvDataReader
{
	/// <summary>
	/// Creates a new CsvDataReader.
	/// </summary>
	/// <param name="filename">The name of a file containing CSV data.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <returns>A CsvDataReader instance.</returns>
	public static CsvDataReader Create(string filename, CsvDataReaderOptions? options = null)
	{
		if (filename == null) throw new ArgumentNullException(nameof(filename));
		// TextReader must be owned when we open it.
		if (options?.OwnsReader == false) throw new CsvConfigurationException();
		var bufferSize = GetIOBufferSize(options);
		var stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan);
		var reader = new StreamReader(stream, Encoding.Default);
		return CreateInternal(reader, null, options);
	}

	/// <summary>
	/// Creates a new CsvDataReader.
	/// </summary>
	/// <param name="reader">The TextReader for the delimited data.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <returns>A CsvDataReader instance.</returns>
	public static CsvDataReader Create(TextReader reader, CsvDataReaderOptions? options = null)
	{
		return CreateInternal(reader, null, options);
	}

	/// <summary>
	/// Creates a new CsvDataReader.
	/// </summary>
	/// <param name="reader">The TextReader for the delimited data.</param>
	/// <param name="buffer">A buffer to use for internal processing.</param>
	/// <param name="options">The options to configure the reader, or null to use the default options.</param>
	/// <returns>A CsvDataReader instance.</returns>
	public static CsvDataReader Create(TextReader reader, char[] buffer, CsvDataReaderOptions? options = null)
	{
		return CreateInternal(reader, buffer, options);
	}

	static CsvDataReader CreateInternal(TextReader reader, char[]? buffer, CsvDataReaderOptions? options)
	{
		options ??= CsvDataReaderOptions.Default;
		if (reader == null) throw new ArgumentNullException(nameof(reader));
		var csv = new CsvDataReader(reader, buffer, options);
		if (!csv.InitializeReader() && options.HasHeaders)
		{
			throw new CsvMissingHeadersException();
		}
		return csv;
	}
	
	bool InitializeReader()
	{
		return this.InitializeReaderAsync().GetAwaiter().GetResult();
	}

	bool NextRecord()
	{
		this.curFieldCount = 0;
		this.recordStart = this.idx;
	start:
		if (this.idx >= bufferEnd)
		{
			FillBuffer();
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
				FillBuffer();
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

			// we were unable to read an entire record out of the buffer synchronously
			if (recordStart == 0 && bufferEnd == buffer.Length)
			{
				if (!GrowBuffer())
				{
					// if we consumed the entire buffer reading this record, then this is an exceptional situation
					// we expect a record to be able to fit entirely within the buffer.
					ThrowRecordTooLarge(fieldIdx);
				}
			}
			else
			{
				FillBuffer();
				// after filling the buffer, we will resume reading fields from where we left off.
			}
		}
	}

	int FillBuffer()
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
		var c = reader.Read(buffer, bufferEnd, count);
		bufferEnd += c;
		if (c == 0)
		{
			atEndOfText = true;
		}
		return c;
	}

	/// <inheritdoc/>
	public override bool Read()
	{		
		this.colCacheIdx = 0;
		if (this.state == State.Open)
		{
			this.rowNumber++;
			var success = this.NextRecord();
			return ReadFinish(success);
		}
		return ReadCommon();
	}

	/// <inheritdoc/>
	public override bool NextResult()
	{
		while (Read()) ;
		return InitializeReader();
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
}
