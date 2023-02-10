using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv;

partial class CsvDataWriter
{
	/// <summary>
	/// Asynchronously writes delimited data.
	/// </summary>
	/// <param name="reader">The DbDataReader to be written.</param>
	/// <param name="cancel">A cancellation token to cancel the asynchronous operation.</param>
	/// <returns>A task representing the asynchronous write operation.</returns>
	public async Task<long> WriteAsync(DbDataReader reader, CancellationToken cancel = default)
	{
		var c = reader.FieldCount;
		var fieldInfos = GetFieldInfos(reader);
		int result;
		var fieldCount = fieldInfos.Length;

		var wc = new WriterContext(this, reader);

		if (writeHeaders)
		{
			for (int i = 0; i < c; i++)
			{
				if (i > 0)
				{
					await WriteDelimiterAsync(0, i, cancel).ConfigureAwait(false);
				}
				var header = reader.GetName(i) ?? string.Empty;

				while ((result = csvWriter.Write(wc, header, this.buffer, pos)) < 0)
				{
					// writing headers will always be flush with start of buffer so we can only grow.
					if (!await GrowBufferAsync(cancel).ConfigureAwait(false))
					{
						// unable to reclaim space
						throw new CsvRecordTooLargeException(0, i);
					}
				}
				pos += result;
			}
			await EndRecordAsync(0, cancel).ConfigureAwait(false);
		}

		int row = 0;
		while (await reader.ReadAsync(cancel).ConfigureAwait(false))
		{
			cancel.ThrowIfCancellationRequested();
			row++;
			c = reader.FieldCount;
			for (var i = 0; i < c; i++)
			{
				if (i > 0)
				{
					if (pos + 1 < buffer.Length)
					{
						// should almost always enter this branch
						// which avoid the async overhead
						buffer[pos++] = delimiter;
					}
					else
					{
						await WriteDelimiterAsync(row, i, cancel).ConfigureAwait(false);
					}
				}
				var field = i < fieldCount ? fieldInfos[i] : Generic;
				if (field.allowNull && await reader.IsDBNullAsync(i, cancel).ConfigureAwait(false))
				{
					continue;
				}

				while ((result = field.writer.Write(wc, i, buffer, pos)) < 0)
				{
					if (!await FlushOrGrowBufferAsync(cancel).ConfigureAwait(false))
					{
						throw new CsvRecordTooLargeException(row, i);
					}
				}
				pos += result;
			}

			await EndRecordAsync(row, cancel).ConfigureAwait(false);
		}
		// flush any pending data on the way out.
		await FlushBufferAsync(cancel).ConfigureAwait(false);
		return row;
	}

	async Task<bool> FlushOrGrowBufferAsync(CancellationToken cancel)
	{
		return
			await FlushBufferAsync(cancel).ConfigureAwait(false) ||
			await GrowBufferAsync(cancel).ConfigureAwait(false);
	}

	async Task<bool> FlushBufferAsync(CancellationToken cancel)
	{
		if (this.recordStart == 0)
		{
			return false;
		}
		else
		{
#if SPAN
			await writer.WriteAsync(buffer.AsMemory(0, recordStart), cancel).ConfigureAwait(false);
#else
			await writer.WriteAsync(buffer, 0, recordStart).ConfigureAwait(false);
#endif
			Array.Copy(buffer, recordStart, buffer, 0, pos - recordStart);
			this.pos -= recordStart;
			this.recordStart = 0;
			return true;
		}
	}

	async Task<bool> GrowBufferAsync(CancellationToken cancel)
	{
		var len = buffer.Length;
		if (maxBufferSize > len)
		{
			var newLen = Math.Min(len * 2, maxBufferSize);
			var newBuffer = new char[newLen];
			if (recordStart > 0)
			{
				await FlushBufferAsync(cancel).ConfigureAwait(false);
			}
			Array.Copy(buffer, recordStart, newBuffer, 0, pos - recordStart);
			this.pos -= recordStart;
			this.recordStart = 0;
			this.buffer = newBuffer;
			return true;
		}
		return false;
	}

	// this should only be called in scenarios where we know there is enough room.
	async Task EndRecordAsync(int row, CancellationToken cancel)
	{
		var nl = this.newLine;

		while (pos + nl.Length >= this.buffer.Length)
		{
			if (!await FlushOrGrowBufferAsync(cancel).ConfigureAwait(false))
			{
				throw new CsvRecordTooLargeException(row, -1);
			}
		}
		for (int i = 0; i < nl.Length; i++)
			buffer[pos++] = nl[i];
		recordStart = pos;
	}

	async Task WriteDelimiterAsync(int row, int col, CancellationToken cancel)
	{
		while (pos + 1 >= this.buffer.Length)
		{
			if (!await FlushOrGrowBufferAsync(cancel).ConfigureAwait(false))
			{
				throw new CsvRecordTooLargeException(row, col);
			}
		}
		buffer[pos++] = delimiter;
	}

#if ASYNC_DISPOSE
	ValueTask IAsyncDisposable.DisposeAsync()
	{
		GC.SuppressFinalize(this);
		if (!disposedValue)
		{
			var task = ((IAsyncDisposable)this.writer).DisposeAsync();
			disposedValue = true;
			return task;
		}
		return new ValueTask(Task.CompletedTask);
	}
#endif

}
