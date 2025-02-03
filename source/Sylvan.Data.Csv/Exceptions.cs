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
/// An exception thrown when an invalid character is found in a CSV file.
/// </summary>
public class CsvInvalidCharacterException : CsvFormatException
{

	internal static CsvInvalidCharacterException Escape(int row, int ordinal, int recordOffset, char invalid)
	{
		return new CsvInvalidCharacterException(row, ordinal, recordOffset, invalid,
			$"Escape character {invalid} was encountered at the end of the text."
		);
	}

	internal static CsvInvalidCharacterException UnescapedQuote(int row, int ordinal, int recordOffset, char invalid)
	{
		return new CsvInvalidCharacterException(row, ordinal, recordOffset, invalid, 
			$"An unescaped quote character '{invalid}' was found inside a quoted field."
		);
	}

	internal static CsvInvalidCharacterException NewRecord(int row, int ordinal, int recordOffset, char invalid)
	{
		return new CsvInvalidCharacterException(row, ordinal, recordOffset, invalid,
			"A delimiter, newline or EOF was expected after a closing quote."
		);
	}

	internal static CsvInvalidCharacterException UnclosedQuote(int row, int ordinal, int recordOffset, char invalid)
	{
		return new CsvInvalidCharacterException(row, ordinal, recordOffset, invalid,
			"A quoted field at the end of the input ends without a closing quote."
		);
	}


	/// <summary>
	/// Gets the character offset into the record where the invalid character was found.
	/// </summary>
	public int RecordOffset { get; }

	/// <summary>
	/// The invalid character encountered.
	/// </summary>
	public char InvalidCharacter { get; }

	internal CsvInvalidCharacterException(int row, int ordinal, int recordOffset, char invalidCharacter, string message) : base(row, ordinal, message)
	{
		this.RecordOffset = recordOffset;
		this.InvalidCharacter = invalidCharacter;
	}
}

/// <summary>
/// The exception that is thrown when the configuration options specify invalid options.
/// </summary>
public class CsvConfigurationException : ArgumentException
{
	internal CsvConfigurationException() : base("The CsvDataReaderOptions specifies invalid options.")
	{ }
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

	internal AmbiguousColumnException(string name) : base($"The CSV file contains more than one column named \"{name}\".")
	{
		this.Name = name;
	}
}
