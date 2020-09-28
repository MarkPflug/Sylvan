using System;
using System.Globalization;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Options for configuring a CsvWriter.
	/// </summary>
	public sealed class CsvWriterOptions
	{
		internal static CsvWriterOptions Default = new CsvWriterOptions();

		const char DefaultDelimiter = ',';
		const char DefaultQuote = '"';
		const char DefaultEscape = '"';
		const int DefaultBufferSize = 0x4000;
		const int MinBufferSize = 0x80;

		/// <summary>
		/// Creates a CsvWriterOptions with the default values.
		/// </summary>
		public CsvWriterOptions()
		{
			this.Delimiter = DefaultDelimiter;
			this.Quote = DefaultQuote;
			this.Escape = DefaultEscape;
			this.NewLine = Environment.NewLine;
			this.BufferSize = DefaultBufferSize;
			this.Culture = CultureInfo.InvariantCulture;
			this.OwnsWriter = true;
			this.TrueString = bool.TrueString;
			this.FalseString = bool.FalseString;
			this.DateFormat = "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'FFFFFFF";
		}

		/// <summary>
		/// The string to write for boolean true values. The default is "True".
		/// </summary>
		public string TrueString { get; set; }

		/// <summary>
		/// The string to write for boolean false values. The default is "False".
		/// </summary>
		public string FalseString { get; set; }

		/// <summary>
		/// The format string used when writing DateTime values. The default is \"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'FFFFFFF\".
		/// </summary>
		public string? DateFormat { get; set; }

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
		/// The string to use for line breaks separating records. The default is Environment.NewLine.
		/// Must be one of "\r", "\n", or "\r\n".
		/// </summary>
		public string NewLine { get; set; }

		/// <summary>
		/// The buffer size to use for writing values. The default is 32kb.
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

		/// <summary>
		/// Indicates if the TextWriter should be closed when the CsvWriter is closed. The default is true.
		/// </summary>
		public bool OwnsWriter { get; set; }

		internal void Validate()
		{
			bool invalid =
				BufferSize < MinBufferSize ||
				char.IsLetterOrDigit(Delimiter) ||
				Quote == Delimiter ||
				(NewLine != "\r" && NewLine != "\n" && NewLine != "\r\n") ||
				TrueString == FalseString ||
				(Buffer != null && Buffer.Length < MinBufferSize);
			if (invalid)
				throw new CsvConfigurationException();
		}
	}
}
