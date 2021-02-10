using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	partial class CsvDataWriter
	{
		/// <summary>
		/// Asynchronously writes delimited data.
		/// </summary>
		/// <param name="reader">The DbDataReader to be written.</param>
		/// <param name="cancel">A cancellation token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous write operation.</returns>
		public virtual async Task<long> WriteAsync(DbDataReader reader, CancellationToken cancel = default)
		{
			var c = reader.FieldCount;
			var fieldTypes = new FieldInfo[c];

			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

			char[] buffer = this.buffer;
			int bufferSize = this.buffer.Length;

			WriteResult result;

			for (int i = 0; i < c; i++)
			{
				var type = reader.GetFieldType(i);
				var allowNull = schema?[i].AllowDBNull ?? true;
				fieldTypes[i] = new FieldInfo(allowNull, type);
			}

			if (writeHeaders)
			{
				for (int i = 0; i < c; i++)
				{
					if (i > 0)
					{
						if (pos + 1 >= bufferSize)
						{
							await FlushBufferAsync().ConfigureAwait(false);
						}
						buffer[pos++] = delimiter;
					}

					var header = reader.GetName(i);
					result = WriteField(header);
					if (result == WriteResult.InsufficientSpace)
					{
						await FlushBufferAsync().ConfigureAwait(false);
						result = WriteField(header);
						if (result == WriteResult.InsufficientSpace)
							throw new CsvRecordTooLargeException(0, i);
					}
				}

				if (pos + 2 >= bufferSize)
				{
					await FlushBufferAsync().ConfigureAwait(false);
				}
				EndRecord();
			}

			int row = 0;
			while (reader.Read())
			{
				cancel.ThrowIfCancellationRequested();
				row++;
				int i = 0; // field

				for (; i < c; i++)
				{
					if (i > 0)
					{
						if (pos + 1 >= bufferSize)
						{
							await FlushBufferAsync().ConfigureAwait(false);
						}
						buffer[pos++] = delimiter;
					}
					for (int retry = 0; retry < 2; retry++)
					{
						var r = WriteField(reader, fieldTypes, i);

						if (r == WriteResult.Complete)
							goto success;
						if (r == WriteResult.InsufficientSpace)
						{
							await FlushBufferAsync().ConfigureAwait(false);
							continue;
						}
					}
					// we arrive here only when there isn't enough room in the buffer
					// to hold the field.				
					throw new CsvRecordTooLargeException(row, i);
				success:;
				}

				if (pos + 2 >= bufferSize)
				{
					await FlushBufferAsync().ConfigureAwait(false);
				}
				EndRecord();
			}
			// flush any pending data on the way out.
			await FlushBufferAsync().ConfigureAwait(false);
			return row;
		}

		async Task FlushBufferAsync()
		{
			if (this.pos == 0) return;
			await writer.WriteAsync(buffer, 0, pos).ConfigureAwait(false);
			pos = 0;
		}

#if NETSTANDARD2_1
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
}
