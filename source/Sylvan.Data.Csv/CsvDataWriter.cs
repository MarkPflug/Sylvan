﻿using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Writes data from a DbDataReader as delimited values to a TextWriter.
	/// </summary>
	public sealed partial class CsvDataWriter
		: IDisposable
#if ASYNC_DISPOSE
		, IAsyncDisposable
#endif
	{
		const int InsufficientSpace = -1;
		const int NeedsQuoting = -2;

		class FieldInfo
		{
			public FieldInfo(bool allowNull, IFieldWriter writer)
			{
				this.allowNull = allowNull;
				this.writer = writer;
			}

			public bool allowNull;
			public IFieldWriter writer;
		}

		IFieldWriter GetWriter(Type type)
		{
			if (type == typeof(string))
			{
				return StringFieldWriter.Instance;
			}
			if (type == typeof(int))
			{
#if SPAN
				return IsFastInt
					? Int32FastFieldWriter.Instance
					: Int32FieldWriter.Instance;
#else
				return Int32FieldWriter.Instance;
#endif
			}

			if (type == typeof(long))
			{
#if SPAN
				return IsFastInt
					? Int64FastFieldWriter.Instance
					: Int64FieldWriter.Instance;
#else
				return Int64FieldWriter.Instance;
#endif
			}
			if (type == typeof(float))
			{
#if SPAN
				return IsFastDouble
					? SingleFastFieldWriter.Instance
					: SingleFieldWriter.Instance;
#else
				return SingleFieldWriter.Instance;
#endif
			}

			if (type == typeof(double))
			{
#if SPAN
				return IsFastDouble
					? DoubleFastFieldWriter.Instance
					: DoubleFieldWriter.Instance;
#else
				return DoubleFieldWriter.Instance;
#endif
			}
			if (type == typeof(decimal))
			{
#if SPAN
				return IsFastDecimal
					? DecimalFastFieldWriter.Instance
					: DecimalFieldWriter.Instance;
#else
				return DecimalFieldWriter.Instance;
#endif
			}
			if (type == typeof(bool))
			{
				return BooleanFieldWriter.Instance;
			}
			if (type == typeof(DateTime))
			{
#if SPAN
				return IsFastDateTime
					? DateTimeFastFieldWriter.Instance
					: DateTimeFieldWriter.Instance;
#else
				return DateTimeFieldWriter.Instance;
#endif
			}

			if (type == typeof(Guid))
			{
#if SPAN
				return IsFastNumeric
					? GuidFastFieldWriter.Instance
					: GuidFieldWriter.Instance;
#else
				return GuidFieldWriter.Instance;
#endif
			}

			if (type == typeof(byte[]))
			{
				if (IsBase64Symbol(quote) || IsBase64Symbol(delimiter) || IsBase64Symbol(escape))
				{
					// if the csv writer is configured to use a symbol that collides with the
					// base64 alphabet, we throw early to avoid creating a potentially ambiguous file.
					// this doesn't happen with default configuration.
					throw new CsvConfigurationException();
				}
				return BinaryFieldWriter.Instance;
			}

			// for everything else fallback to GetValue/ToString
			return ObjectFieldWriter.Instance;
		}

#if SPAN

		bool IsInvariantCulture
		{
			get
			{
				return this.culture == CultureInfo.InvariantCulture;
			}
		}

		// indicates if the writer is configured such that
		// most primitive data types can be written without escaping.
		bool IsFastConfig
		{
			get
			{
				return
					(
					this.delimiter == ',' ||
					this.delimiter == '\t' ||
					this.delimiter == '|' ||
					this.delimiter == ';' ||
					this.delimiter < ' '
					)
					&&
					(
					this.quote == '\"' ||
					this.quote == '\'' ||
					this.quote < ' '
					)
					&&
					(
					this.escape == '\\' ||
					this.escape == '\"' ||
					this.escape == '\'' ||
					this.escape < ' '
					);
			}
		}

		bool IsFastNumeric => IsInvariantCulture && IsFastConfig;

		bool IsFastDecimal => IsFastNumeric;

		bool IsFastDouble => IsFastNumeric;

		bool IsFastInt => IsFastNumeric;

		bool IsFastDateTime
		{
			get
			{
				return IsInvariantCulture && IsFastConfig
					&& this.dateFormat == CsvDataWriterOptions.Default.DateFormat
					&& this.dateTimeFormat == CsvDataWriterOptions.Default.DateTimeFormat;
			}
		}

#endif

		// Size of the buffer used for base64 encoding, must be a multiple of 3.
		const int Base64EncSize = 3 * 256;

		readonly TextWriter writer;
		readonly CsvWriter csvWriter;

		readonly bool writeHeaders;
		readonly char delimiter;
		readonly char quote;
		readonly char escape;
		readonly char comment;
		readonly string newLine;

		readonly string trueString;
		readonly string falseString;
		readonly string? dateTimeFormat;
		readonly string? dateFormat;

		readonly CultureInfo culture;

		byte[] dataBuffer = Array.Empty<byte>();
		readonly char[] buffer;
		int pos;

		bool disposedValue;

		readonly bool[] needsEscape;

		/// <summary>
		/// Creates a new CsvDataWriter.
		/// </summary>
		/// <param name="fileName">The path of the file to write.</param>
		/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
		public static CsvDataWriter Create(string fileName, CsvDataWriterOptions? options = null)
		{
			options = options ?? CsvDataWriterOptions.Default;
			var writer = new StreamWriter(fileName, false, Encoding.UTF8);
			return new CsvDataWriter(writer, options);
		}

		/// <summary>
		/// Creates a new CsvDataWriter.
		/// </summary>
		/// <param name="writer">The TextWriter to receive the delimited data.</param>
		/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
		public static CsvDataWriter Create(TextWriter writer, CsvDataWriterOptions? options = null)
		{
			options = options ?? CsvDataWriterOptions.Default;
			return new CsvDataWriter(writer, options);
		}

		CsvDataWriter(TextWriter writer, CsvDataWriterOptions? options = null)
		{
			options = options ?? CsvDataWriterOptions.Default;
			options.Validate();
			this.writer = writer ?? throw new ArgumentNullException(nameof(writer));
			this.csvWriter = options.Style == CsvStyle.Standard ? CsvWriter.Quoted : CsvWriter.Escaped;
			this.trueString = options.TrueString;
			this.falseString = options.FalseString;
			this.dateTimeFormat = options.DateTimeFormat;
			this.dateFormat = options.DateFormat;
			this.writeHeaders = options.WriteHeaders;
			this.delimiter = options.Delimiter;
			this.quote = options.Quote;
			this.escape = options.Escape;
			this.comment = options.Comment;
			this.newLine = options.NewLine;
			this.culture = options.Culture;
			this.buffer = options.Buffer ?? new char[options.BufferSize];
			this.pos = 0;

			// create a lookup of all the characters that need to be escaped.
			this.needsEscape = new bool[128];
			Flag(delimiter);
			Flag(quote);
			Flag(escape);
			if (options.Style == CsvStyle.Escaped)
			{
				Flag(comment);
			}
			Flag('\r');
			Flag('\n');
		}

		void Flag(char c)
		{
			// these characters are already validated to be in 0-127
			needsEscape[c] = true;
		}

		// this should only be called in scenarios where we know there is enough room.
		void EndRecord()
		{
			var nl = this.newLine;
			for (int i = 0; i < nl.Length; i++)
				buffer[pos++] = nl[i];
		}

		static bool IsBase64Symbol(char c)
		{
			return c == '+' || c == '/' || c == '=';
		}
	}
}
