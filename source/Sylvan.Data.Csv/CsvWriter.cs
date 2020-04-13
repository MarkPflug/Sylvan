using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	public sealed class CsvWriter :
		IDisposable
#if NETSTANDARD2_1
		, IAsyncDisposable
#endif
	{
		readonly TextWriter writer;
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
			this.delimiter = options.Delimiter;
			this.quote = options.Quote;
			this.escape = options.Escape;
			this.newLine = options.NewLine;
			this.culture = options.Culture;
			this.defaultCulture = this.culture == CultureInfo.InvariantCulture;
			this.prepareBuffer = new char[0x100];
			this.bufferSize = options.BufferSize;
			this.writeBuffer = new char[bufferSize];
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
			await writer.WriteAsync(writeBuffer, 0, pos);
			pos = 0;
		}

		void FlushBuffer()
		{
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

		WriteResult WriteValueOptimistic(string str)
		{
			if (pos + str.Length >= bufferSize) return WriteResult.NeedsFlush; //TODO: benchmark without this check.

			for (int i = 0; i < str.Length; i++)
			{
				var c = str[i];
				if (c == delimiter || c == '\r' || c == '\n' || c == quote)
					return WriteResult.Pessimistic;
				if (pos == bufferSize)
					return WriteResult.NeedsFlush;
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
			if (defaultCulture)
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

		public async Task EndRecordAsync()
		{
			if (!WriteNewLine())
			{
				await FlushBufferAsync();
				WriteNewLine();
			}
			fieldIdx = 0;
		}

		public void Flush()
		{
			FlushBuffer();
		}

		public Task FlushAsync()
		{
			return FlushBufferAsync();
		}

		public async Task WriteFieldAsync(bool value)
		{
			if (pos + 6 >= bufferSize)
			{
				await FlushBufferAsync();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			var str = value ? "true" : "false";
			fieldIdx++;
			if (WriteValueOptimistic(str) == WriteResult.Okay)
				return;

			WriteValue(str);
		}

		public async Task WriteFieldAsync(double value)
		{
			if (pos + 32 >= bufferSize)
			{
				await FlushBufferAsync();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		public async Task WriteFieldAsync(long value)
		{
			if (pos + 32 >= bufferSize)
			{
				await FlushBufferAsync();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		public async Task WriteFieldAsync(int value)
		{
			if (pos + 32 >= bufferSize)
			{
				await FlushBufferAsync();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		public async Task WriteFieldAsync(DateTime value)
		{
			if (pos + 64 >= bufferSize)
			{
				await FlushBufferAsync();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

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
			if (string.IsNullOrEmpty(value))
				return;

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


		public void EndRecord()
		{
			if (!WriteNewLine())
			{
				FlushBuffer();
				WriteNewLine();
			}
			fieldIdx = 0;
		}

		public void WriteField(bool value)
		{
			if (pos + 6 >= bufferSize)
			{
				FlushBuffer();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			var str = value ? "true" : "false";
			fieldIdx++;
			if (WriteValueOptimistic(str) == WriteResult.Okay)
				return;

			WriteValue(str);
		}

		public void WriteField(float value)
		{
			if (pos + 32 >= bufferSize)
			{
				FlushBuffer();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		public void WriteField(double value)
		{
			if (pos + 32 >= bufferSize)
			{
				FlushBuffer();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		public void WriteField(int value)
		{
			if (pos + 32 >= bufferSize)
			{
				FlushBuffer();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

		public void WriteField(DateTime value)
		{
			if (pos + 64 >= bufferSize)
			{
				FlushBuffer();
			}
			if (fieldIdx > 0)
			{
				writeBuffer[pos++] = delimiter;
			}
			fieldIdx++;
			if (WriteValueOptimistic(value) == WriteResult.Okay)
				return;

			WriteValue(value);
		}

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
			if (string.IsNullOrEmpty(value))
				return;

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
				disposedValue = true;
			}
		}

		void IDisposable.Dispose()
		{
			Dispose(true);
		}

#if NETSTANDARD2_1
		ValueTask IAsyncDisposable.DisposeAsync()
		{
			if (!disposedValue)
			{
				return new ValueTask(this.FlushAsync());
			}
			
			return new ValueTask(Task.CompletedTask);
		}
#endif

#endregion
	}
}
