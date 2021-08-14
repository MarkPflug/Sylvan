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
	/// A callback handler to receive comments read from a CSV file.
	/// </summary>
	public abstract class CommentHandler
	{
		// NOTE: This class will probably appear overkill. Why not just pass a delegate?
		// My intent is to allow this to potentially handle Span<char> overload in the future.
		// Being implemented this way allows the code to remain binary backward compatible in
		// that event. Add a virtual span-based method that delegates to the string-based abstract implementation.
		// if the span-based is overridden, then it can avoid the allocation. Overkill? Oh, most definitely.

		/// <summary>
		/// The method that is called when comments are read.
		/// </summary>
		/// <param name="reader">The CsvDataReader.</param>
		/// <param name="comment">The comment string.</param>
		public abstract void HandleComment(CsvDataReader reader, string comment);

		/// <summary>
		/// Implicitly casts an Action to a CommentHandler.
		/// </summary>
		/// <param name="handler">The method to recive the comment.</param>
		public static implicit operator CommentHandler(Action<CsvDataReader, string> handler)
		{
			return new StringCommentHandler(handler);
		}

		/// <summary>
		/// Implicitly casts an Action to a CommentHandler.
		/// </summary>
		/// <param name="handler">The method to recive the comment.</param>
		public static implicit operator CommentHandler(Action<string> handler)
		{
			return new StringCommentHandler((r, c) => handler(c));
		}

		sealed class StringCommentHandler : CommentHandler
		{
			Action<CsvDataReader, string> cb;
			public StringCommentHandler(Action<CsvDataReader, string> cb)
			{
				this.cb = cb;
			}

			public override void HandleComment(CsvDataReader reader, string comment)
			{
				cb(reader, comment);
			}
		}
	}

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
	/// Specifies how result sets are interpreted within a CSV file.
	/// </summary>
	public enum ResultSetMode
	{
		/// <summary>
		/// The entire file is interpreted as a single result set.
		/// </summary>
		SingleResult = 1,

		/// <summary>
		/// In multi result mode a new result set is identified by a change in column count.
		/// Empty lines are skipped between result sets.
		/// </summary>
		MultiResult = 2,		
	}

	/// <summary>
	/// Options for configuring a CsvDataReader.
	/// </summary>
	public sealed class CsvDataReaderOptions
	{
		internal static CsvDataReaderOptions Default = new CsvDataReaderOptions();

		const char DefaultQuote = '"';
		const char DefaultEscape = '"';
		const char DefaultComment = '#';
		const int DefaultBufferSize = 0x4000;
		const int MinBufferSize = 0x80;

		char? delimiter;

		/// <summary>
		/// Creates a CsvDataReaderOptions with the default values.
		/// </summary>
		public CsvDataReaderOptions()
		{
			this.HasHeaders = true;
			this.delimiter = null;
			this.CsvStyle = CsvStyle.Standard;
			this.Quote = DefaultQuote;
			this.Escape = DefaultEscape;
			this.Comment = DefaultComment;
			this.BufferSize = DefaultBufferSize;
			this.HeaderComparer = StringComparer.Ordinal;
			this.Culture = CultureInfo.InvariantCulture;
			this.Schema = null;
			this.OwnsReader = true;

			this.TrueString = null;
			this.FalseString = null;
			this.DateFormat = null;

			this.StringFactory = null;
			this.CommentHandler = null;
			this.BinaryEncoding = BinaryEncoding.Base64;
			this.ResultSetMode = ResultSetMode.SingleResult;
		}

		/// <summary>
		/// Indicates the behavior of result transitions.
		/// </summary>
		public ResultSetMode ResultSetMode { get; set; }

		/// <summary>
		/// Indicates the binary encoding that should be used when writing binary columns.
		/// </summary>
		public BinaryEncoding BinaryEncoding { get; set; }

		/// <summary>
		/// A string factory function which can de-dupe strings on construction. Defaults to null.
		/// </summary>
		public StringFactory? StringFactory { get; set; }

		/// <summary>
		/// A callback method which will be called when a comment is found in the CSV.
		/// </summary>
		public CommentHandler? CommentHandler { get; set; }

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
		/// Specifies the character used to indicate a comment. Defaults to '#'.
		/// </summary>
		public char Comment { get; set; }

		/// <summary>
		/// Indicates the CSV parsing style, defaults to Standard.
		/// </summary>
		public CsvStyle CsvStyle { get; set; }

		/// <summary>
		/// Specifies the character used for escaping characters in quoted fields. Defaults to '"'.
		/// </summary>
		public char Escape { get; set; }

		/// <summary>
		/// The size of buffer to use when reading records.
		/// A record must fit within a single buffer, otherwise an exception is thrown.
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
				(Buffer != null && Buffer.Length < MinBufferSize) ||
				Delimiter >= 128 ||
				Quote >= 128 ||
				Escape >= 128;
			if (invalid)
				throw new CsvConfigurationException();
		}
	}
}
