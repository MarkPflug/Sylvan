using System;
using System.Data.Common;

namespace Sylvan.Data.Csv
{
	partial class CsvDataWriter
	{
		/// <summary>
		/// Synchronously writes delimited data.
		/// </summary>
		/// <param name="reader">The DbDataReader to be written.</param>
		/// <returns>A task representing the asynchronous write operation.</returns>
		public long Write(DbDataReader reader)
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
							FlushBuffer();
						}
						buffer[pos++] = delimiter;
					}

					var header = reader.GetName(i) ?? "";
					result = csvWriter.Write(wc, header, buffer, pos);
					if (result < 0)
					{
						FlushBuffer();
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
					FlushBuffer();
				}
				EndRecord();
			}

			int row = 0;
			while (reader.Read())
			{
				row++;
				int i = 0; // field

				for (; i < c; i++)
				{
					if (i > 0)
					{
						if (pos + 1 >= bufferSize)
						{
							FlushBuffer();
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
							FlushBuffer();
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
					FlushBuffer();
				}
				EndRecord();
			}
			// flush any pending data on the way out.
			FlushBuffer();
			return row;
		}

		void FlushBuffer()
		{
			if (this.pos == 0) return;
			writer.Write(buffer, 0, pos);
			pos = 0;
		}

		void IDisposable.Dispose()
		{
			GC.SuppressFinalize(this);
			if (!disposedValue)
			{
				this.writer.Dispose();
				disposedValue = true;
			}
		}
	}
}
