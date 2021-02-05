using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	partial class CsvDataReader
	{
		/// <summary>
		/// Creates a new CsvDataReader asynchronously.                                                                                          
		/// </summary>
		/// <param name="filename">The name of a file containing CSV data.</param>
		/// <param name="options">The options to configure the reader, or null to use the default options.</param>
		/// <returns>A task representing the asynchronous creation of a CsvDataReader instance.</returns>
		public static async Task<CsvDataReader> CreateAsync(string filename, CsvDataReaderOptions? options = null)
		{
			if (filename == null) throw new ArgumentNullException(nameof(filename));
			// TextReader must be owned when we open it.
			if (options?.OwnsReader == false) throw new CsvConfigurationException();

			var bufferSize = options?.BufferSize ?? options?.Buffer?.Length ?? CsvDataReaderOptions.Default.BufferSize;
			bufferSize = Math.Max(bufferSize, Environment.SystemPageSize);
			var stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize * 2, FileOptions.SequentialScan | FileOptions.Asynchronous);
			var reader = new StreamReader(stream, Encoding.Default, true, bufferSize);
			var csv = new CsvDataReader(reader, options);
			await csv.InitializeAsync(options?.Schema);
			return csv;
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

		async Task<int> FillBufferAsync()
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
			var c = await reader.ReadBlockAsync(buffer, bufferEnd, count);
			bufferEnd += c;
			if (c < count)
			{
				atEndOfText = true;
			}
			return c;
		}


		/// <inheritdoc/>
		public override Task<bool> ReadAsync(CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();
			this.rowNumber++;
			if (this.state == State.Open)
			{
				return this.NextRecordAsync();
			}
			else if (this.state == State.Initialized)
			{
				// after initizialization, the first record would already be in the buffer
				// if hasRows is true.
				if (hasRows)
				{
					this.state = State.Open;
					return CompleteTrue;
				}
				else
				{
					this.state = State.End;
				}
			}
			this.rowNumber = -1;
			return CompleteFalse;
		}

		/// <inheritdoc/>
		public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
		{
			state = State.End;
			return CompleteFalse;
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
