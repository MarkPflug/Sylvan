using System;
using System.Globalization;

namespace Sylvan.Data.Csv;

/// <summary>
/// Specifies how strings are quoted
/// </summary>
[Flags]
public enum CsvStringQuoting
{
	/// <summary>
	/// Strings are only quoted when it is required.
	/// </summary>
	Default = 0,

	/// <summary>
	/// Empty strings are always quoted. This distinguishes them from null.
	/// </summary>
	AlwaysQuoteEmpty = 1,

	/// <summary>
	/// Non-empty strings are always quoted. This helps prevent confusion with numbers and dates.
	/// </summary>
	AlwaysQuoteNonEmpty = 2,

	/// <summary>
	/// All strings are always quoted.
	/// </summary>
	AlwaysQuote = 3,
}

/// <summary>
/// Options for configuring a CsvWriter.
/// </summary>
public sealed class CsvDataWriterOptions
{
	internal CsvDataWriterOptions Clone()
	{
		return (CsvDataWriterOptions)MemberwiseClone();
	}

	internal static readonly CsvDataWriterOptions Default = new();

	internal const char DefaultDelimiter = ',';
	internal const char DefaultQuote = '"';
	internal const char DefaultEscape = '"';
	internal const char DefaultComment = '#';
	const int DefaultBufferSize = 0x4000;
	const int MinBufferSize = 0x80;

	/// <summary>
	/// Creates a CsvWriterOptions with the default values.
	/// </summary>
	public CsvDataWriterOptions()
	{
		this.BinaryEncoding = BinaryEncoding.Base64;
		this.Style = CsvStyle.Standard;
		this.Delimiter = DefaultDelimiter;
		this.QuoteStrings = CsvStringQuoting.Default;
		this.Quote = DefaultQuote;
		this.Escape = DefaultEscape;
		this.Comment = DefaultComment;
		this.NewLine = "\n";
		this.BufferSize = DefaultBufferSize;
		this.Culture = CultureInfo.InvariantCulture;
		this.TrueString = bool.TrueString;
		this.FalseString = bool.FalseString;
		this.WriteHeaders = true;
	}

	/// <summary>
	/// Indicates the encoding format to use when writing binary columns.
	/// </summary>
	public BinaryEncoding BinaryEncoding { get; set; }

	/// <summary>
	/// Indicates if the header row should be written.
	/// </summary>
	public bool WriteHeaders { get; set; }

	/// <summary>
	/// The string to write for boolean true values. The default is "True".
	/// </summary>
	public string TrueString { get; set; }

	/// <summary>
	/// The string to write for boolean false values. The default is "False".
	/// </summary>
	public string FalseString { get; set; }

	/// <summary>
	/// The format string used when writing DateTime values. The default is \"O\".
	/// </summary>
	public string? DateTimeFormat { get; set; }

	/// <summary>
	/// The format string used when writing DateTimeOffset values. The default is \"O\".
	/// </summary>
	public string? DateTimeOffsetFormat { get; set; }

	/// <summary>
	/// The format string used when writing DateTime values, or DateOnly values on supported frameworks, that have a time component. The default is \"O\".
	/// </summary>
	[Obsolete("Use DateTimeFormat instead")]
	public string? DateFormat
	{
		get => this.DateTimeFormat;
		set => this.DateTimeFormat = value;
	}

	/// <summary>
	/// The format string used when writing TimeSpan values that have to time component. The default is \"c\".
	/// </summary>
	public string? TimeSpanFormat { get; set; }

#if NET6_0_OR_GREATER

	/// <summary>
	/// The format string used when writing DateTime values, or DateOnly values on supported frameworks, that have a time component. The default is \"O\".
	/// </summary>
	public string? DateOnlyFormat { get; set; }

	/// <summary>
	/// The format string used when writing TimeOnly values.
	/// </summary>
	public string? TimeOnlyFormat { get; set; }

	/// <summary>
	/// The format string used when writing TimeSpan values that have to time component. The default is \"O\".
	/// </summary>
	[Obsolete("Use TimeOnlyFormat instead.")] 
	public string? TimeFormat { 

		get => TimeOnlyFormat; 
		set => TimeOnlyFormat = value; 
	}

#endif

	/// <summary>
	/// The style of the CSV file to be written, defaults to <see cref="CsvStyle.Standard"/>.
	/// </summary>
	public CsvStyle Style { get; set; }

	/// <summary>
	/// The delimiter to use between fields. The default is ','.
	/// </summary>
	public char Delimiter { get; set; }

	/// <summary>
	/// The rules to use when quoting strings, defaults to <see cref="CsvStringQuoting.Default"/>
	/// </summary>
	public CsvStringQuoting QuoteStrings { get; set; }

	/// <summary>
	/// Empty strings will be written as empty quotes in the CSV.
	/// This allows distinguishing empty strings from null.
	/// </summary>
	[Obsolete("Use QuoteStrings instead.")]
	public bool QuoteEmptyStrings
	{
		get => QuoteStrings.HasFlag(CsvStringQuoting.AlwaysQuoteEmpty);
		set
		{
			if (value)
				QuoteStrings |= CsvStringQuoting.AlwaysQuoteEmpty;
			else
				QuoteStrings &= ~CsvStringQuoting.AlwaysQuoteEmpty;
		}
	}

	/// <summary>
	/// The character to use for quoting fields. The default is '"'.
	/// </summary>
	public char Quote { get; set; }

	/// <summary>
	/// The character to use for escaping in quoted fields fields. The default is '"'.
	/// </summary>
	public char Escape { get; set; }

	/// <summary>
	/// The character used to indicate a comment line. The default is '#'.
	/// </summary>
	public char Comment { get; set; }

	/// <summary>
	/// The string to use for line breaks separating records. The default is "\n".
	/// Must be one of "\r", "\n", or "\r\n".
	/// </summary>
	public string NewLine { get; set; }

	/// <summary>
	/// The buffer size to use for writing values.
	/// </summary>
	public int BufferSize { get; set; }

	/// <summary>
	/// The maximum size the internal buffer can grow to.
	/// </summary>
	public int? MaxBufferSize { get; set; }

	/// <summary>
	/// The buffer to use when writing records.
	/// The default is null, in which case the writer will allocate a buffer of BufferSize.
	/// </summary>
	[Obsolete("Use the buffer parameter to CsvDataWriter.Create instead.")]
	public char[]? Buffer { get; set; }

	/// <summary>
	/// The CultureInfo to use when writing values. The default is the InvariantCulture.
	/// </summary>
	public CultureInfo Culture { get; set; }

	internal void Validate()
	{
		bool invalid =
			BufferSize < MinBufferSize ||
			char.IsLetterOrDigit(Delimiter) ||
			Quote == Delimiter ||
			(NewLine != "\r" && NewLine != "\n" && NewLine != "\r\n") ||
			TrueString == FalseString ||
#pragma warning disable CS0618 // Type or member is obsolete
			(Buffer != null && Buffer.Length < MinBufferSize) ||
#pragma warning restore CS0618 // Type or member is obsolete
			Delimiter >= 128 ||
			Quote >= 128 ||
			Escape >= 128 ||
			Comment >= 128 ||
			(QuoteStrings != CsvStringQuoting.Default && Style == CsvStyle.Escaped)
			;
		if (invalid)
			throw new CsvConfigurationException();
	}
}
