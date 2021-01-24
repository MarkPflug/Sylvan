﻿using System;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// The exception that is thrown when CSV data is malformed.
	/// </summary>
	public class CsvFormatException : Exception
	{
		internal CsvFormatException(int row, int ordinal, string? msg, Exception? inner) 
			: base(msg, inner)
		{
			this.RowNumber = row;
			this.FieldOrdinal = ordinal;
		}

		/// <summary>
		/// The row number containing the malformed data.
		/// </summary>
		public int RowNumber { get; }

		/// <summary>
		/// The ordinal of the field containing the malformed data.
		/// </summary>
		public int FieldOrdinal { get; }
	}

	///// <summary>
	///// The exception thrown when encountering an invalid character while
	///// decoding binary data fields.
	///// </summary>
	//public class CsvInvalidEncodingException : FormatException
	//{
	//	/// <summary>
	//	/// The offset into the field where the invalid character was located.
	//	/// </summary>
	//	public int FieldOffset { get; }

	//	/// <summary>
	//	/// The invalid character value.
	//	/// </summary>
	//	public char InvalidCharacter { get; }

	//	internal CsvInvalidEncodingException(int row, int ordinal, int fieldOffset, char c)
	//		: base (row, ordinal, null, null)
	//	{
	//		this.FieldOffset = fieldOffset;
	//		this.InvalidCharacter = c;
	//	}
	//}

	/// <summary>
	/// The exception that is thrown when the configuration options specify invalid options.
	/// </summary>
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
		internal CsvRecordTooLargeException(int row, int ordinal, string? msg, Exception? inner) 
			: base(row, ordinal, msg, inner) { }
	}

	/// <summary>
	/// The exception that is thrown when reading an empty CSV when headers are expected.
	/// </summary>
	public sealed class CsvMissingHeadersException : CsvFormatException
	{
		internal CsvMissingHeadersException() : base(0, 0, null, null) { }
	}

	/// <summary>
	/// The exception thrown when trying to get the ordinal for a column
	/// name that appears more than once.
	/// </summary>
	public sealed class AmbiguousColumnException : ArgumentException
	{
		/// <summary>
		/// Gets the the ambiguous column name.
		/// </summary>
		public string Name { get; }

		internal AmbiguousColumnException(string name)
		{
			this.Name = name;
		}
	}
}
