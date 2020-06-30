using System;
using System.Globalization;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Options for configuring a CsvDataReader.
	/// </summary>
	public sealed class CsvDataReaderOptions
	{
		internal static CsvDataReaderOptions Default = new CsvDataReaderOptions();

		const char DefaultDelimiter = ',';
		const char DefaultQuote = '"';
		const char DefaultEscape = '"';
		const int DefaultBufferSize = 0x10000;
		const int MinBufferSize = 0x80;

		/// <summary>
		/// Creates a CsvDataReaderOptions with the default values.
		/// </summary>
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
			this.OwnsReader = true;
		}

		/// <summary>
		/// Specifies if the CSV data contains a header row with column names. Defaults to true.
		/// </summary>
		public bool HasHeaders { get; set; }

		/// <summary>
		/// Specifies the field delimiter. Defaults to ','.
		/// </summary>
		public char Delimiter { get; set; }

		/// <summary>
		/// Specifies the character used for quoting fields. Defaults to '"'.
		/// </summary>
		public char Quote { get; set; }

		/// <summary>
		/// Specifies the character used for escaping characters in quoted fields. Defaults to '"'.
		/// </summary>
		public char Escape { get; set; }

		/// <summary>
		/// The size of buffer to use when reading records.
		/// A record must fit within a single buffer, otherwise an exception is thrown.
		/// The default buffer size is 64kb.
		/// </summary>
		public int BufferSize { get; set; }

		/// <summary>
		/// The StringComparer to use when looking up header values.
		/// Can be used to allow case-insensitive column lookup.
		/// The default is Ordinal.
		/// </summary>
		public StringComparer HeaderComparer { get; set; }

		/// <summary>
		/// The CultureInfo to use when parsing values in the CSV data.
		/// Defaults to the InvariantCulture.
		/// </summary>
		public CultureInfo Culture { get; set; }

		/// <summary>
		/// Indicates if the TextReader should be closed when the CsvDataReader is closed. The default is true.
		/// </summary>
		public bool OwnsReader { get; set; }

		/// <summary>
		/// Allows specifying a strongly-typed schema for the CSV data.
		/// </summary>
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
