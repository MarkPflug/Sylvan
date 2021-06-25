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
		public async Task<long> WriteAsync(DbDataReader reader, CancellationToken cancel = default)
		{
			var c = reader.FieldCount;
			var fieldInfos = new FieldInfo[c];

			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

			char[] buffer = this.buffer;
			int bufferSize = this.buffer.Length;

			int result;

			for (int i = 0; i < c; i++)
			{
				var type = reader.GetFieldType(i);
				var allowNull = schema?[i].AllowDBNull ?? true;
				var writer = GetWriter(type);
				fieldInfos[i] = new FieldInfo(allowNull, writer);
			}

			var wc = new WriterContext(this, reader);

			if (writeHeaders)
			{
				for (int i = 0; i < c; i++)
				{
					if (i > 0)
					{
						if (pos + 1 >= bufferSize)
						{
							await FlushBufferAsync(cancel).ConfigureAwait(false);
						}
						buffer[pos++] = delimiter;
					}

					var header = reader.GetName(i) ?? "";
					result = csvWriter.Write(wc, header, buffer, pos);
					if (result < 0)
					{
						await FlushBufferAsync(cancel).ConfigureAwait(false);
						result = csvWriter.Write(wc, header, buffer, pos);
						if (result < 0)
						{
							throw new CsvRecordTooLargeException(0, i);
						}
						else
						{
							pos += result;
						}
					}
					else
					{
						pos += result;
					}
				}

				if (pos + 2 >= bufferSize)
				{
					await FlushBufferAsync(cancel).ConfigureAwait(false);
				}
				EndRecord();
			}

			int row = 0;
			while (await reader.ReadAsync(cancel))
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
							await FlushBufferAsync(cancel).ConfigureAwait(false);
						}
						buffer[pos++] = delimiter;
					}
					var field = fieldInfos[i];
					if (field.allowNull && reader.IsDBNull(i))
					{
						continue;
					}

					for (int retry = 0; retry < 2; retry++)
					{
						var r = field.writer.Write(wc, i, buffer, pos);

						if (r >= 0)
						{
							pos += r;
							goto success;
						}
						else
						{
							await FlushBufferAsync(cancel).ConfigureAwait(false);
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
					await FlushBufferAsync(cancel).ConfigureAwait(false);
				}
				EndRecord();
			}
			// flush any pending data on the way out.
			await FlushBufferAsync(cancel).ConfigureAwait(false);
			return row;
		}

		async Task FlushBufferAsync(CancellationToken cancel)
		{
			if (this.pos == 0) return;
#if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
			var m = buffer.AsMemory().Slice(0, pos);
			await writer.WriteAsync(m, cancel).ConfigureAwait(false);
#else
			await writer.WriteAsync(buffer, 0, pos).ConfigureAwait(false);
#endif
			pos = 0;
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
}
