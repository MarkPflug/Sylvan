using System;
using System.Globalization;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// A function that can be used to de-dupe strings during construction directly from internal buffers.
	/// </summary>
	/// <remarks>
	/// The Sylvan.Common library can provide an implementation of this method via the Sylvan.StringPool type.
	/// </remarks>
	public delegate string StringFactory(char[] buffer, int offset, int length);

	/// <summary>
	/// Gets the binary encoding used when reading fields with GetBytes.
	/// </summary>
	public enum BinaryEncoding
	{
		/// <summary>
		/// Reads values as base64, does not support whitespace in values.
		/// </summary>
		Base64 = 1,

		/// <summary>
		/// Read values as hexadecimal, allows optional "0x" prefix.
		/// </summary>
		Hexadecimal = 2,
	}

	/// <summary>
	/// Options for configuring a CsvDataReader.
	/// </summary>
	public sealed class CsvDataReaderOptions
	{
		internal static CsvDataReaderOptions Default = new CsvDataReaderOptions();

		const char DefaultQuote = '"';
		const char DefaultEscape = '"';
		const int DefaultBufferSize = 0x10000;
		const int MinBufferSize = 0x80;

		char? delimiter;

		/// <summary>
		/// Creates a CsvDataReaderOptions with the default values.
		/// </summary>
		public CsvDataReaderOptions()
		{
			this.HasHeaders = true;
			this.delimiter = null;
			this.ImplicitQuotes = false;
			this.Quote = DefaultQuote;
			this.Escape = DefaultEscape;
			this.BufferSize = DefaultBufferSize;
			this.HeaderComparer = StringComparer.Ordinal;
			this.Culture = CultureInfo.InvariantCulture;
			this.Schema = null;
			this.OwnsReader = true;

			this.TrueString = null;
			this.FalseString = null;
			this.DateFormat = null;

			this.StringFactory = null;
			this.BinaryEncoding = BinaryEncoding.Base64;
		}

		/// <summary>
		/// 
		/// </summary>
		public BinaryEncoding BinaryEncoding { get; set; }

		/// <summary>
		/// A string factory function which can de-dupe strings on construction. Defaults to null.
		/// </summary>
		public StringFactory? StringFactory { get; set; }

		/// <summary>
		/// The string which represents true values when reading boolean. Defaults to null.
		/// </summary>
		public string? TrueString { get; set; }

		/// <summary>
		/// The string which represents false values when reading boolean. Defaults to null.
		/// </summary>
		public string? FalseString { get; set; }

		/// <summary>
		/// The format string to use to parse dates. Defaults to null, in which case standard date parsing rules apply.
		/// </summary>
		public string? DateFormat { get; set; }

		/// <summary>
		/// Specifies if the CSV data contains a header row with column names. Defaults to true.
		/// </summary>
		public bool HasHeaders { get; set; }

		/// <summary>
		/// Specifies the field delimiter. By default, uses autodetect.
		/// </summary>
		/// <remarks>
		/// Setting the delimiter will disable auto-detection.
		/// </remarks>
		public char? Delimiter
		{
			get
			{
				return this.delimiter;
			}
			set
			{
				this.delimiter = value;
			}
		}

		/// <summary>
		/// Specifies the character used for quoting fields. Defaults to '"'.
		/// </summary>
		public char Quote { get; set; }

		/// <summary>
		/// Indicates if the file uses escaping only, meaning all fields are implicitly quoted. Defaults to false.
		/// </summary>
		public bool ImplicitQuotes { get; set; }

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
		/// The buffer to use when reading records.
		/// The default is null, in which case the reader will allocate the buffer.
		/// </summary>
		public char[]? Buffer { get; set; }

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
				(Delimiter != null && char.IsLetterOrDigit(Delimiter.Value)) ||
				Delimiter == Quote ||
				BufferSize < MinBufferSize ||
				(StringComparer.OrdinalIgnoreCase.Equals(TrueString, FalseString) && TrueString != null) ||
				(Buffer != null && Buffer.Length < MinBufferSize);
			if (invalid)
				throw new CsvConfigurationException();
		}
	}
}
