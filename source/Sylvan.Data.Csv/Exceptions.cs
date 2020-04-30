using System;

namespace Sylvan.Data.Csv
{
	public class CsvFormatException : Exception
	{
		internal CsvFormatException(long row, int ordinal, string? msg, Exception? inner) 
			: base(msg, inner)
		{
			this.RowNumber = row;
			this.FieldOrdinal = ordinal;
		}

		public long RowNumber { get; }

		public int FieldOrdinal { get; }
	}

	public class CsvConfigurationException : ArgumentException
	{
		internal CsvConfigurationException() { }
	}

	/// <summary>
	/// The exception that is thrown when a single CSV record is too large to fit in the read buffer.
	/// </summary>
	/// <remarks>
	/// CsvRecordTooLargeException is typically an indication of a incorrectly formatted CSV file.
	/// Most CSV files should easily fit the constraints of the default buffer size. It is possible
	/// to increase the buffer size for exceptional cases
	/// </remarks>
	public sealed class CsvRecordTooLargeException : CsvFormatException
	{
		internal CsvRecordTooLargeException(long row, int ordinal, string? msg, Exception? inner) 
			: base(row, ordinal, msg, inner) { }
	}
}
