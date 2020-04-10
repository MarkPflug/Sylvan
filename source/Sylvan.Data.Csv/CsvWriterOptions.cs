using System;
using System.Globalization;

namespace Sylvan.Data.Csv
{
	public sealed class CsvWriterOptions
	{
		internal static CsvWriterOptions Default = new CsvWriterOptions();

		const char DefaultDelimiter = ',';
		const char DefaultQuote = '"';
		const char DefaultEscape = '"';
		const int DefaultBufferSize = 0x8000;
		const int MinBufferSize = 0x80;

		public CsvWriterOptions()
		{
			this.Delimiter = DefaultDelimiter;
			this.Quote = DefaultQuote;
			this.Escape = DefaultEscape;
			this.NewLine = Environment.NewLine;
			this.BufferSize = DefaultBufferSize;
			this.Culture = CultureInfo.InvariantCulture;
		}

		public char Delimiter { get; set; }
		public char Quote { get; set; }
		public char Escape { get; set; }
		public string NewLine { get; set; }
		public int BufferSize { get; set; }
		public CultureInfo Culture { get; set; }

		internal void Validate()
		{
			bool invalid =
				BufferSize < MinBufferSize ||
				char.IsLetterOrDigit(Delimiter) ||
				Quote == Delimiter ||
				(NewLine != "\r" && NewLine != "\n" && NewLine != "\r\n");
			if (invalid)
				throw new CsvConfigurationException();
		}
	}
}
