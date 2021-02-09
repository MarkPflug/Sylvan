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
		public virtual long Write(DbDataReader reader)
		{
			var c = reader.FieldCount;
			var fieldTypes = new FieldInfo[c];

			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

			char[] buffer = this.buffer;
			int bufferSize = this.bufferSize;

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
							FlushBuffer();
						}
						buffer[pos++] = delimiter;
					}

					var header = reader.GetName(i);
					result = WriteField(header);
					if (result == WriteResult.InsufficientSpace)
					{
						FlushBuffer();
						result = WriteField(header);
						if (result == WriteResult.InsufficientSpace)
							throw new Exception();
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
					for (int retry = 0; retry < 2; retry++)
					{
						var r = WriteField(reader, fieldTypes, i);

						if (r == WriteResult.Complete)
							goto success;
						if (r == WriteResult.InsufficientSpace)
						{
							FlushBuffer();
							continue;
						}
					}
					// we arrive here only when there isn't enough room in the buffer
					// to hold the field.				
					throw new Exception();
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
