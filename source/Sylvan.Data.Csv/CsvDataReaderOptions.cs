using System;
using System.Globalization;

namespace Sylvan.Data.Csv
{
	public sealed class CsvDataReaderOptions
	{
		internal static CsvDataReaderOptions Default = new CsvDataReaderOptions();

		const char DefaultDelimiter = ',';
		const char DefaultQuote = '"';
		const char DefaultEscape = '"';
		const int DefaultBufferSize = 0x10000;
		const int MinBufferSize = 0x80;

		public CsvDataReaderOptions()
		{
			this.HasHeaders = true;
			this.Delimiter = DefaultDelimiter;
			this.Quote = DefaultQuote;
			this.Escape = DefaultEscape;
			this.BufferSize = DefaultBufferSize;
			this.HeaderComparer = StringComparer.Ordinal;
			this.Culture = CultureInfo.InvariantCulture;
			this.Schema = null;
		}

		public bool HasHeaders { get; set; }
		public char Delimiter { get; set; }
		public char Quote { get; set; }
		public char Escape { get; set; }
		public int BufferSize { get; set; }
		public StringComparer HeaderComparer { get; set; }
		public CultureInfo Culture { get; set; }
		public ICsvSchemaProvider? Schema { get; set; }

		internal void Validate()
		{
			var invalid =
				char.IsLetterOrDigit(Delimiter) ||
				Delimiter == Quote ||
				BufferSize < MinBufferSize;
			if (invalid)
				throw new CsvConfigurationException();
		}
	}
}
