using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Writes delimited data to a TextWriter.
	/// </summary>
	public sealed class CsvWriter :
		IDisposable
#if NETSTANDARD2_1
		, IAsyncDisposable
#endif
	{
		readonly TextWriter writer;
		readonly string trueString;
		readonly string falseString;
		readonly char delimiter;
		readonly char quote;
		readonly char escape;
		readonly string newLine;
		readonly CultureInfo culture;
		char[] prepareBuffer;
		readonly char[] writeBuffer;
		readonly int bufferSize;
		int pos;
		int fieldIdx;
		bool defaultCulture;
		readonly bool ownsWriter;

		/// <summary>
		/// Creates a CsvWriter.
		/// </summary>
		/// <param name="writer">The TextWriter to receive the delimited data.</param>
		/// <param name="options">The options to configure the writer.</param>
		public CsvWriter(TextWriter writer, CsvWriterOptions? options = null)
		{
			if (writer == null) throw new ArgumentNullException(nameof(writer));
			if (options != null)
			{
				options.Validate();
			}
			else
			{
				options = CsvWriterOptions.Default;
			}

			this.writer = writer;
			this.trueString = options.TrueString;
			this.falseString = options.FalseString;
			this.delimiter = options.Delimiter;
			this.quote = options.Quote;
			this.escape = options.Escape;
			this.newLine = options.NewLine;
			this.culture = options.Culture;
			this.defaultCulture = this.culture == CultureInfo.InvariantCulture;
			this.prepareBuffer = new char[0x100];
			this.bufferSize = options.BufferSize;
			this.writeBuffer = new char[bufferSize];
			this.ownsWriter = options.OwnsWriter;
			this.pos = 0;
		}

		(int o, int l) PrepareValue(string str)
		{
			var worstCaseLenth = str.Length * 2 + 2;
			// at worst, we'll have to escape every character and put quotes around it

			if (this.prepareBuffer.Length < worstCaseLenth)
			{
				if (str.Length > bufferSize)
				{
					// the value being written is too large to fit in the buffer
					throw new ArgumentOutOfRangeException();
				}
				Array.Resize(ref this.prepareBuffer, worstCaseLenth);
			}
			var buffer = this.prepareBuffer;
			var p = 0;
			buffer[p++] = quote;
			bool isQuoted = false;
			for (int i = 0; i < str.Length; i++)
			{
				var c = str[i];

				if (c == delimiter || c == '\r' || c == '\n' || c == quote)
				{
					isQuoted = true;
					if (c == quote || c == escape)
					{
						buffer[p++] = escape;
					}
				}
				buffer[p++] = c;
			}
			buffer[p++] = quote;

			return isQuoted
				? (0, p)
				: (1, p - 2);
		}

		async Task FlushBufferAsync()
		{
			if (this.pos == 0) return;
			await writer.WriteAsync(writeBuffer, 0, pos);
			pos = 0;
		}

		void FlushBuffer()
		{
			if (this.pos == 0) return;
			writer.Write(writeBuffer, 0, pos);
			pos = 0;
		}

		enum WriteResult
		{
			None,
			NeedsFlush,
			Pessimistic,
			Okay,
		}

		WriteResult WriteValue(string str)
		{
			var (o, l) = PrepareValue(str);
			return WriteValue(this.prepareBuffer, o, l);
		}

		WriteResult WriteValue(char[] buffer, int o, int l)
		{
			if (pos + l >= bufferSize) return WriteResult.NeedsFlush;

			Array.Copy(buffer, o, this.writeBuffer, pos, l);
			pos += l;
			return WriteResult.Okay;
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public void WriteField(Guid value)
		{
			StartField(64);

			// keep it fast/allocation-free in the sane case.
			if (delimiter == '-' || quote == '-')
			{
				WriteValue(value.ToString());
			}
			else
			{
				WriteValueInvariant(value);
			}
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public void WriteField(byte[] value)
		{
			this.WriteField(value, 0, value.Length);
		}

		WriteResult WriteValueOptimistic(string str)
		{
			var start = pos;
			if (pos + str.Length >= bufferSize) return WriteResult.NeedsFlush; //TODO: benchmark without this check.

			for (int i = 0; i < str.Length; i++)
			{
				var c = str[i];
				if (c == delimiter || c == '\r' || c == '\n' || c == quote)
				{
					pos = start;
					return WriteResult.Pessimistic;
				}
				if (pos == bufferSize)
				{
					pos = start;
					return WriteResult.NeedsFlush;
				}
				writeBuffer[pos++] = c;
			}
			return WriteResult.Okay;
		}

		WriteResult WriteValueInvariant(int value)
		{
#if NETSTANDARD2_1
			var span = writeBuffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Okay;
			}
			return WriteResult.NeedsFlush;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(int value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.Pessimistic;
		}

		WriteResult WriteValue(int value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString();
			return WriteValue(str);
		}

		WriteResult WriteValueInvariant(long value)
		{
#if NETSTANDARD2_1
			var span = writeBuffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Okay;
			}
			return WriteResult.NeedsFlush;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(long value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.Pessimistic;
		}

		WriteResult WriteValue(long value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString(culture);
			return WriteValue(str);
		}

		WriteResult WriteValueInvariant(DateTime value)
		{
#if NETSTANDARD2_1
			var span = writeBuffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Okay;
			}
			return WriteResult.NeedsFlush;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueInvariant(Guid value)
		{
#if NETSTANDARD2_1
			var span = writeBuffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c))
			{
				pos += c;
				return WriteResult.Okay;
			}
			return WriteResult.NeedsFlush;
#else
			var str = value.ToString();
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(DateTime value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.Pessimistic;
		}

		WriteResult WriteValue(DateTime value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString(culture);
			return WriteValue(str);
		}

		WriteResult WriteValueInvariant(float value)
		{
#if NETSTANDARD2_1
			var span = writeBuffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Okay;
			}
			return WriteResult.NeedsFlush;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(float value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.Pessimistic;
		}

		WriteResult WriteValue(float value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString(culture);
			return WriteValue(str);
		}

		WriteResult WriteValueInvariant(double value)
		{
#if NETSTANDARD2_1
			var span = writeBuffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Okay;
			}
			return WriteResult.NeedsFlush;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(double value)
		{
			if (defaultCulture && delimiter != '.')
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.Pessimistic;
		}

		WriteResult WriteValue(double value)
		{
			if (defaultCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString(culture);
			return WriteValue(str);
		}

		bool WriteNewLine()
		{
			if (pos + this.newLine.Length >= writeBuffer.Length)
				return false;
			for (int i = 0; i < this.newLine.Length; i++)
				writeBuffer[pos++] = this.newLine[i];
			return true;
		}

		/// <summary>
		/// Asynchronously writes the end of the current record.
		/// </summary>
		/// <returns>A task representing the asynchronous write.</returns>
		public async Task EndRecordAsync()
		{
			if (!WriteNewLine())
			{
				await FlushBufferAsync();
				WriteNewLine();
			}
			fieldIdx = 0;
		}

		/// <summary>
		/// Flushes any pending data to the output writer.
		/// </summary>
		public void Flush()
		{
			FlushBuffer();
		}

		/// <summary>
		/// Asynchronously flushes any pending data to the output writer.
		/// </summary>
		/// <returns>A task representing the asynchronous operation.</returns>
		public Task FlushAsync()
		{
			return FlushBufferAsync();
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(bool value)
		{
			await StartFieldAsync(8);
			var str = value ? trueString : falseString;
			if (WriteValueOptimistic(str) == WriteResult.Okay)
				return;

			WriteValue(str);
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(double value)
		{
			await StartFieldAsync(32);
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(long value)
		{
			if (pos + 32 >= bufferSize)
			{
				await FlushBufferAsync();
			}

			StartField();

			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		void StartField()
		{
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
		}

		void StartField(int size)
		{
			FlushIfNeeded(size);
			StartField();
		}

		void FlushIfNeeded(int size)
		{
			if (pos + size >= bufferSize)
			{
				FlushBuffer();
			}
		}

		async Task FlushIfNeededAsync(int size)
		{
			if (pos + size >= bufferSize)
			{
				await FlushBufferAsync();
			}
		}

		async Task StartFieldAsync(int size)
		{
			await FlushIfNeededAsync(size);
			StartField();
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(int value)
		{
			await StartFieldAsync(32);
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(DateTime value)
		{
			await StartFieldAsync(64);

			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(string? value)
		{
			bool optimistic = true;
			if (fieldIdx > 0)
			{
				while (true)
				{
					if (pos + 1 < bufferSize)
					{
						writeBuffer[pos++] = delimiter;
						break;
					}
					else
					{
						await FlushBufferAsync();
					}
				}
			}
			fieldIdx++;
#if NETSTANDARD2_1
			if (string.IsNullOrEmpty(value))
				return;
#else
			// to shut up the C# null checker
			if (value == null || value.Length == 0)
				return;
#endif

			goto field;
		flush:
			await FlushBufferAsync();
		field:
			if (optimistic)
			{
				switch (WriteValueOptimistic(value))
				{
					case WriteResult.Okay:
						return;
					case WriteResult.NeedsFlush:
						goto flush;
				}
			}

			if (WriteValue(value) == WriteResult.NeedsFlush)
				goto flush;
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(Guid value)
		{
			await StartFieldAsync(64);

			// keep it fast/allocation-free in the sane case.
			if (delimiter == '-' || quote == '-')
			{
				WriteValue(value.ToString());
			}
			else
			{
				WriteValueInvariant(value);
			}
		}

		/// <summary>
		/// Asynchronously writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		/// <returns>A task representing the asynchronous operation.</returns>
		public async Task WriteFieldAsync(byte[] value)
		{
			await WriteFieldAsync(value, 0, value.Length);
		}

		/// <summary>
		/// Writes hex-encoded data to the current record.
		/// </summary>
		/// <param name="buffer">The buffer containing the data to write.</param>
		/// <param name="offset">The offset in the buffer from which to begin writing.</param>
		/// <param name="length">The number of bytes to be written.</param>
		public async Task WriteFieldAsync(byte[] buffer, int offset, int length)
		{
			var size = (length * 4 / 3) + 1;

			if (size > writeBuffer.Length)
			{
				throw new ArgumentOutOfRangeException(nameof(length));
			}

			await StartFieldAsync(size);
			AssertBinaryPrereq();
			var len = Convert.ToBase64CharArray(buffer, offset, length, this.writeBuffer, pos, Base64FormattingOptions.None);
			pos += len;
		}

		void AssertBinaryPrereq()
		{
			// punt these crazy scenarios.
			if (delimiter == '+' || delimiter == '/' || delimiter == '=')
			{
				throw new InvalidOperationException();
			}

			if (quote == '+' || quote == '/' || quote == '=')
			{
				throw new InvalidOperationException();
			}
		}

		/// <summary>
		/// Writes the end of the curent record.
		/// </summary>
		public void EndRecord()
		{
			if (!WriteNewLine())
			{
				FlushBuffer();
				WriteNewLine();
			}
			fieldIdx = 0;
		}

		/// <summary>
		/// Writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		public void WriteField(bool value)
		{
			StartField(8);

			var str = value ? trueString : falseString;
			if (WriteValueOptimistic(str) == WriteResult.Okay)
				return;

			WriteValue(str);
		}

		/// <summary>
		/// Writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		public void WriteField(float value)
		{
			StartField(32);
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		/// <summary>
		/// Writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		public void WriteField(double value)
		{
			StartField(64);
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		/// <summary>
		/// Writes hex-encoded data to the current record.
		/// </summary>
		/// <param name="buffer">The buffer containing the data to write.</param>
		/// <param name="offset">The offset in the buffer from which to begin writing.</param>
		/// <param name="length">The number of bytes to be written.</param>
		public void WriteField(byte[] buffer, int offset, int length)
		{
			var size = (length * 4 / 3) + 1;

			if (size > writeBuffer.Length)
			{
				throw new ArgumentOutOfRangeException(nameof(length));
			}

			StartField(size);

			AssertBinaryPrereq();

			var len = Convert.ToBase64CharArray(buffer, offset, length, this.writeBuffer, pos, Base64FormattingOptions.None);
			pos += len;
		}

		/// <summary>
		/// Writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		public void WriteField(int value)
		{
			StartField(32);
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		/// <summary>
		/// Writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		public void WriteField(DateTime value)
		{
			StartField(64);
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		/// <summary>
		/// Writes a value to the current record.
		/// </summary>
		/// <param name="value">The value to write.</param>
		public void WriteField(string? value)
		{
			bool optimistic = true;
			if (fieldIdx > 0)
			{
				while (true)
				{
					if (pos + 1 < bufferSize)
					{
						writeBuffer[pos++] = delimiter;
						break;
					}
					else
					{
						FlushBuffer();
					}
				}
			}
			fieldIdx++;
#if NETSTANDARD2_1
			if (string.IsNullOrEmpty(value))
				return;
#else
			if (value == null || value.Length == 0)
				return;
#endif

			goto field;
		flush:
			FlushBuffer();
		field:
			if (optimistic)
			{
				switch (WriteValueOptimistic(value))
				{
					case WriteResult.Okay:
						return;
					case WriteResult.NeedsFlush:
						goto flush;
				}
			}

			if (WriteValue(value) == WriteResult.NeedsFlush)
				goto flush;
			return;
		}

		internal async Task StartBinaryFieldAsync()
		{
			AssertBinaryPrereq();
			await FlushBufferAsync();
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
		}

		internal int ContinueBinaryField(byte[] buffer, int len)
		{
			var written = Convert.ToBase64CharArray(buffer, 0, len, writeBuffer, pos);
			pos += written;
			return written;
		}

		#region IDisposable Support
		bool disposedValue = false;

		void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					this.Flush();
				}
				if (ownsWriter)
				{
					this.writer.Dispose();
				}
				disposedValue = true;
			}
		}

		void IDisposable.Dispose()
		{
			Dispose(true);
		}

#if NETSTANDARD2_1
		async ValueTask IAsyncDisposable.DisposeAsync()
		{
			if (!disposedValue)
			{
				await this.FlushAsync();
				if (ownsWriter)
				{
					await this.writer.DisposeAsync();
				}
				disposedValue = true;
			}
		}
#endif

		#endregion
	}
}
