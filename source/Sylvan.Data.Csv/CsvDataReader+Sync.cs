using System;

namespace Sylvan.Data.Csv
{
	partial class CsvDataReader
	{
		bool NextRecord()
		{
			this.curFieldCount = 0;
			this.recordStart = this.idx;

			if (this.idx >= bufferEnd)
			{
				FillBuffer();
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
						FillBuffer();
						// after filling the buffer, we will resume reading fields from where we left off.
					}
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
			var c = reader.ReadBlock(buffer, bufferEnd, count);
			bufferEnd += c;
			if (c < count)
			{
				atEndOfText = true;
			}
			return c;
		}


		/// <inheritdoc/>
		public override bool Read()
		{
			this.rowNumber++;
			if (this.state == State.Open)
			{
				return this.NextRecord();
			}
			else if (this.state == State.Initialized)
			{
				// after initizialization, the first record would already be in the buffer
				// if hasRows is true.
				if (hasRows)
				{
					this.state = State.Open;
					return true;
				}
				else
				{
					this.state = State.End;
				}
			}
			this.rowNumber = -1;
			return false;
		}

		/// <inheritdoc/>
		public override bool NextResult()
		{
			state = State.End;
			return false;
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
}
