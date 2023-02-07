using System;
using System.Collections.ObjectModel;
using System.Data.Common;

namespace Sylvan.Data.Csv;

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
		ReadOnlyCollection<DbColumn>? schema = null;
		try
		{
			// on .NET 48, this will throw NSE
			// on that runtime, we'll treat it as null.
			schema = reader.GetColumnSchema();
		}
		catch (NotSupportedException)
		{
			schema = null;
		}
		int result;

		for (int i = 0; i < c; i++)
		{
			var allowNull = true;
			if (schema != null && i < schema.Count)
			{
				allowNull = schema[i].AllowDBNull ?? true;
			}

			var writer = GetWriter(reader, i);
			fieldInfos[i] = new FieldInfo(allowNull, writer);
		}

		var fieldCount = fieldInfos.Length;

		var wc = new WriterContext(this, reader);

		if (writeHeaders)
		{
			for (int i = 0; i < c; i++)
			{
				if (i > 0)
				{
					WriteDelimiter(0, i);
				}
				var header = reader.GetName(i) ?? "";

				while ((result = csvWriter.Write(wc, header, this.buffer, pos)) < 0)
				{
					// writing headers will always be flush with start of buffer so we can only grow.
					if (!GrowBuffer())
					{
						// unable to reclaim space
						throw new CsvRecordTooLargeException(0, i);
					}
				}
				pos += result;
			}
			EndRecord(0);
		}

		int row = 0;
		while (reader.Read())
		{
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
						WriteDelimiter(row, i);
					}
				}
				var field = i < fieldCount ? fieldInfos[i] : FieldInfo.Generic;
				if (field.allowNull && reader.IsDBNull(i))
				{
					continue;
				}

				while ((result = field.writer.Write(wc, i, buffer, pos)) < 0)
				{
					if (!FlushOrGrowBuffer())
					{
						throw new CsvRecordTooLargeException(row, i);
					}
				}
				pos += result;
			}

			EndRecord(row);
		}
		// flush any pending data on the way out.
		FlushBuffer();
		return row;
	}

	bool FlushOrGrowBuffer()
	{
		return FlushBuffer() || GrowBuffer();
	}

	bool FlushBuffer()
	{
		if (this.recordStart == 0)
		{
			return false;
		}
		else
		{
			writer.Write(buffer, 0, recordStart);
			Array.Copy(buffer, recordStart, buffer, 0, pos - recordStart);
			this.pos -= recordStart;
			this.recordStart = 0;
			return true;
		}
	}

	bool GrowBuffer()
	{
		var len = buffer.Length;
		if (maxBufferSize > len)
		{
			var newLen = Math.Min(len * 2, maxBufferSize);
			var newBuffer = new char[newLen];
			if (recordStart > 0)
			{
				FlushBuffer();
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
	void EndRecord(int row)
	{
		var nl = this.newLine;

		while (pos + nl.Length >= this.buffer.Length)
		{
			if (!FlushOrGrowBuffer())
			{
				throw new CsvRecordTooLargeException(row, -1);
			}
		}
		for (int i = 0; i < nl.Length; i++)
			buffer[pos++] = nl[i];
		recordStart = pos;
	}

	void WriteDelimiter(int row, int col)
	{
		while (pos + 1 >= this.buffer.Length)
		{
			if (!FlushOrGrowBuffer())
			{
				throw new CsvRecordTooLargeException(row, col);
			}
		}
		buffer[pos++] = delimiter;
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
