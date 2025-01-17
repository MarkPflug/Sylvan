using System;

namespace Sylvan.Data.Csv;

/// <summary>
/// The exception that is thrown when CSV data is malformed.
/// </summary>
public class CsvFormatException : FormatException
{
	internal CsvFormatException(int row, int ordinal, string message, Exception? inner = null)
		: base(message, inner)
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
	internal CsvRecordTooLargeException(int row, int ordinal)
		: base(row, ordinal, $"Row {row} was too large. Try increasing the MaxBufferSize setting.") { }
}

/// <summary>
/// The exception that is thrown when reading an empty CSV when headers are expected.
/// </summary>
public sealed class CsvMissingHeadersException : CsvFormatException
{
	internal CsvMissingHeadersException()
		: base(0, 0, "The CSV file does not have headers, but the HasHeaders option was set to true.") { }
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
