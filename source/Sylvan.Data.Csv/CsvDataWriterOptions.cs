using System;
using System.Globalization;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Options for configuring a CsvWriter.
	/// </summary>
	public sealed class CsvDataWriterOptions
	{
		internal CsvDataWriterOptions Clone()
		{
			return (CsvDataWriterOptions)MemberwiseClone();
		}

		internal static CsvDataWriterOptions Default = new CsvDataWriterOptions();

		const char DefaultDelimiter = ',';
		const char DefaultQuote = '"';
		const char DefaultEscape = '"';
		const char DefaultComment = '#';
		const int DefaultBufferSize = 0x4000;
		const int MinBufferSize = 0x80;

		/// <summary>
		/// Creates a CsvWriterOptions with the default values.
		/// </summary>
		public CsvDataWriterOptions()
		{
			this.Style = CsvStyle.Standard;
			this.Delimiter = DefaultDelimiter;
			this.Quote = DefaultQuote;
			this.Escape = DefaultEscape;
			this.Comment = DefaultComment;
			this.NewLine = Environment.NewLine;
			this.BufferSize = DefaultBufferSize;
			this.Culture = CultureInfo.InvariantCulture;
			this.TrueString = bool.TrueString;
			this.FalseString = bool.FalseString;
			this.DateTimeFormat = null;
			this.DateFormat = this.DateTimeFormat;
			this.TimeSpanFormat = null;
#if NET6_0_OR_GREATER
			this.TimeFormat = null;
#endif
			this.WriteHeaders = true;
		}

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
		/// The format string used when writing DateTime values, or DateOnly values on supported frameworks, that have to time component. The default is \"O\".
		/// </summary>
		public string? DateFormat { get; set; }

		/// <summary>
		/// The format string used when writing TimeSpan values that have to time component. The default is \"c\".
		/// </summary>
		public string? TimeSpanFormat { get; set; }

#if NET6_0_OR_GREATER
		/// <summary>
		/// The format string used when writing TimeSpan values that have to time component. The default is \"O\".
		/// </summary>
		public string? TimeFormat { get; set; }
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
		/// The string to use for line breaks separating records. The default is Environment.NewLine.
		/// Must be one of "\r", "\n", or "\r\n".
		/// </summary>
		public string NewLine { get; set; }

		/// <summary>
		/// The buffer size to use for writing values.
		/// </summary>
		public int BufferSize { get; set; }

		/// <summary>
		/// The buffer to use when writing records.
		/// The default is null, in which case the writer will allocate a buffer of BufferSize.
		/// </summary>
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
				(Buffer != null && Buffer.Length < MinBufferSize) ||
				Delimiter >= 128 ||
				Quote >= 128 ||
				Escape >= 128 ||
				Comment >= 128;
			if (invalid)
				throw new CsvConfigurationException();
		}
	}
}
