using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;

namespace Sylvan.Data.Csv;

/// <summary>
/// Writes data from a DbDataReader as delimited values to a TextWriter.
/// </summary>
public sealed partial class CsvDataWriter
	: IDisposable
#if ASYNC_DISPOSE
	, IAsyncDisposable
#endif
{
	const int InsufficientSpace = int.MinValue;
	const int NeedsQuoting = -2;

	static readonly bool[] DefaultEscapeFlags;

	static CsvDataWriter()
	{
		DefaultEscapeFlags = new bool[128];
		DefaultEscapeFlags[CsvDataWriterOptions.DefaultDelimiter] = true;
		DefaultEscapeFlags[CsvDataWriterOptions.DefaultQuote] = true;
		DefaultEscapeFlags[CsvDataWriterOptions.DefaultEscape] = true;
		DefaultEscapeFlags['\n'] = true;
		DefaultEscapeFlags['\r'] = true;
	}

	ObjectFieldWriter? objectWriter;
	ObjectFieldWriter ObjectWriter
	{
		get { return objectWriter ??= new ObjectFieldWriter(this); }
	}

	FieldInfo? generic;
	FieldInfo Generic
	{
		get { return generic ??= new FieldInfo(true, ObjectWriter); }
	}

	class FieldInfo
	{
		public FieldInfo(bool allowNull, FieldWriter writer)
		{
			this.allowNull = allowNull;
			this.writer = writer;
		}

		public bool allowNull;
		public FieldWriter writer;
	}

	FieldInfo[] GetFieldInfos(DbDataReader reader)
	{
		var c = reader.FieldCount;
		var fieldInfos = new FieldInfo[c];
		ReadOnlyCollection<DbColumn>? schema = reader.CanGetColumnSchema() ? reader.GetColumnSchema() : null;
		for (int i = 0; i < c; i++)
		{
			var allowNull = true;
			if (schema != null && i < schema.Count)
			{
				allowNull = schema[i].AllowDBNull ?? true;
			}

			var writer = GetWriter(reader, i);
			fieldInfos[i] = new FieldInfo(allowNull, writer);
		}
		return fieldInfos;
	}

	FieldWriter GetWriter(DbDataReader reader, int ordinal)
	{
		var type = reader.GetFieldType(ordinal);
		return GetWriter(type);
	}

	FieldWriter GetWriter(Type type)
	{
		if (type == null)
		{
			return new ObjectFieldWriter(this);
		}

		if (type == typeof(string))
		{
			return StringFieldWriter.Instance;
		}
		if (type == typeof(byte))
		{
#if SPAN
			return IsFastNumeric
				? ByteFastFieldWriter.Instance
				: ByteFieldWriter.Instance;
#else
			return ByteFieldWriter.Instance;
#endif
		}

		if (type == typeof(short))
		{
#if SPAN
			return IsFastNumeric
				? Int16FastFieldWriter.Instance
				: Int16FieldWriter.Instance;
#else
			return Int16FieldWriter.Instance;
#endif
		}

		if (type == typeof(int))
		{
#if SPAN
			return IsFastNumeric
				? Int32FastFieldWriter.Instance
				: Int32FieldWriter.Instance;
#else
			return Int32FieldWriter.Instance;
#endif
		}

		if (type == typeof(long))
		{
#if SPAN
			return IsFastNumeric
				? Int64FastFieldWriter.Instance
				: Int64FieldWriter.Instance;
#else
			return Int64FieldWriter.Instance;
#endif
		}
		if (type == typeof(float))
		{
#if SPAN
			return IsFastNumeric
				? SingleFastFieldWriter.Instance
				: SingleFieldWriter.Instance;
#else
			return SingleFieldWriter.Instance;
#endif
		}

		if (type == typeof(double))
		{
#if SPAN
			return IsFastNumeric
				? DoubleFastFieldWriter.Instance
				: DoubleFieldWriter.Instance;
#else
			return DoubleFieldWriter.Instance;
#endif
		}
		if (type == typeof(decimal))
		{
#if SPAN
			return IsFastNumeric
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
			if (IsFastDateTime)
			{
				return DateTimeIsoFastFieldWriter.Instance;
			}
			else
			{
				return this.dateTimeFormat == null
					? DateTimeIsoFieldWriter.Instance
					: DateTimeFormatFieldWriter.Instance;
			}
#else
			return DateTimeFormatFieldWriter.Instance;
#endif
		}

		if (type == typeof(DateTimeOffset))
		{
#if SPAN
			if (IsFastDateTimeOffset)
			{
				return DateTimeOffsetIsoFastFieldWriter.Instance;
			}
			else
			{
				return this.dateTimeOffsetFormat == null
					? DateTimeOffsetIsoFieldWriter.Instance
					: DateTimeOffsetFormatFieldWriter.Instance;
			}
#else
			return DateTimeOffsetFormatFieldWriter.Instance;
#endif
		}

		if (type == typeof(Guid))
		{
#if SPAN
			return IsFastConfig
				? GuidFastFieldWriter.Instance
				: GuidFieldWriter.Instance;
#else
			return GuidFieldWriter.Instance;
#endif
		}

		if (type == typeof(byte[]))
		{
			if (this.binaryEncoding == BinaryEncoding.Base64)
			{
				if (IsBase64Symbol(quote) || IsBase64Symbol(delimiter) || IsBase64Symbol(escape))
				{
					// if the csv writer is configured to use a symbol that collides with the
					// base64 alphabet, we throw early to avoid creating a potentially ambiguous file.
					// this doesn't happen with default configuration.
					throw new CsvConfigurationException();
				}
				return BinaryBase64FieldWriter.Instance;
			}
			else
			{
				return BinaryHexFieldWriter.Instance;
			}
		}

		if (type == typeof(TimeSpan))
		{
#if SPAN
			return IsFastTimeSpan
				? TimeSpanFastFieldWriter.Instance
				: TimeSpanFieldWriter.Instance;
#else
			return TimeSpanFieldWriter.Instance;
#endif

		}

#if NET6_0_OR_GREATER // SPAN implied

		if (type == typeof(DateOnly))
		{
			if (IsFastDateOnly)
			{
				return DateOnlyIsoFastFieldWriter.Instance;
			}
			else
			{
				return this.dateOnlyFormat == null
					? DateOnlyIsoFieldWriter.Instance
					: DateOnlyFormatFieldWriter.Instance;
			}
		}

		if (type == typeof(TimeOnly))
		{
			if (this.timeOnlyFormat == null)
			{
				return IsFastTimeOnly
					? TimeOnlyFastFieldWriter.Instance
					: TimeOnlyFormatFieldWriter.Instance;
			}
			else
			{
				return TimeOnlyFormatFieldWriter.Instance;
			}
		}

#endif

#if SPAN
		if (type.IsEnum)
		{
			if (!EnumMap.TryGetValue(type, out FieldWriter? writer))
			{
				if (IsCandidateEnum(type))
				{
					// multiple calls to MakeGenericType return the same instance.
					var prop = typeof(EnumFastFieldWriter<>).MakeGenericType(type).GetField("Instance", BindingFlags.Static | BindingFlags.Public)!;
					writer = (FieldWriter)prop.GetValue(null)!;
				}
				else
				{
					writer = this.ObjectWriter;
				}
				EnumMap.TryAdd(type, writer);
			}

			return writer;
		}

#endif

		// for everything else fallback to generic object handler
		return this.ObjectWriter;
	}

#if SPAN

	static readonly ConcurrentDictionary<Type, FieldWriter> EnumMap = new();

	// determines if the type is an enum type that can be
	// efficiently optimized.
	static bool IsCandidateEnum(Type type)
	{
		if (!type.IsEnum)
		{
			return false;
		}
		if (type.GetEnumUnderlyingType() != typeof(int))
		{
			return false;
		}
		if (type.GetCustomAttribute(typeof(FlagsAttribute)) != null)
		{
			return false;
		}

		Array arr = Enum.GetValues(type);
		for (int i = 0; i < arr.Length; i++)
		{
			var val = (int)arr.GetValue(i)!;
			if (val < 0 || val >= 0x100)
			{
				return false;
			}
		}
		return true;
	}

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
				this.escape == '\"' ||
				this.escape == '\\' ||
				this.escape == '\'' ||
				this.escape < ' '
				);
		}
	}

	bool IsFastNumeric =>
		IsInvariantCulture &&
		IsFastConfig;

	bool IsFastDateTime
	{
		get
		{
			return
				IsFastNumeric &&
				this.dateTimeFormat == null;
		}
	}

	bool IsFastDateTimeOffset
	{
		get
		{
			return IsInvariantCulture && IsFastConfig
				&& this.dateTimeOffsetFormat == CsvDataWriterOptions.Default.DateTimeOffsetFormat;
		}
	}

	bool IsFastTimeSpan
	{
		get
		{
			return
				IsFastNumeric &&
				this.timeSpanFormat == null;
		}
	}

#if NET6_0_OR_GREATER

	bool IsFastDateOnly
	{
		get
		{
			return
				IsFastNumeric &&
				this.dateOnlyFormat == null;
		}
	}

	bool IsFastTimeOnly
	{
		get
		{
			return
				IsFastNumeric &&
				this.timeOnlyFormat == null;
		}
	}

#endif

#endif

	// Size of the buffer used for base64 encoding, must be a multiple of 3.
	const int Base64EncSize = 3 * 256;

	readonly TextWriter writer;
	readonly CsvWriter csvWriter;

	readonly bool writeHeaders;
	readonly bool quoteEmpty;
	readonly bool quoteNonEmpty;
	readonly char delimiter;
	readonly char quote;
	readonly char escape;
	readonly char comment;
	readonly string newLine;
	readonly BinaryEncoding binaryEncoding;

	readonly string trueString;
	readonly string falseString;
	readonly string? dateTimeFormat;
	readonly string? dateTimeOffsetFormat;
	readonly string? timeSpanFormat;
#if NET6_0_OR_GREATER
	readonly string? timeOnlyFormat;
	readonly string? dateOnlyFormat;
#endif

	readonly CultureInfo culture;

	readonly int maxBufferSize;
	byte[] dataBuffer = Array.Empty<byte>();
	char[] buffer;
	int pos;
	int recordStart = 0;

	bool disposedValue;

	readonly bool[] needsEscape;

	/// <summary>
	/// Creates a new CsvDataWriter.
	/// </summary>
	/// <param name="fileName">The path of the file to write.</param>
	/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
	public static CsvDataWriter Create(string fileName, CsvDataWriterOptions? options = null)
	{
		options ??= CsvDataWriterOptions.Default;
		var writer = new StreamWriter(fileName, false, Encoding.UTF8);
		return new CsvDataWriter(writer, null, options);
	}

	/// <summary>
	/// Creates a new CsvDataWriter.
	/// </summary>
	/// <param name="writer">The TextWriter to receive the delimited data.</param>
	/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
	public static CsvDataWriter Create(TextWriter writer, CsvDataWriterOptions? options = null)
	{
		options ??= CsvDataWriterOptions.Default;
		return new CsvDataWriter(writer, null, options);
	}

	/// <summary>
	/// Creates a new CsvDataWriter.
	/// </summary>
	/// <param name="writer">The TextWriter to receive the delimited data.</param>
	/// <param name="buffer">A buffer to use for internal processing.</param>
	/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
	public static CsvDataWriter Create(TextWriter writer, char[] buffer, CsvDataWriterOptions? options = null)
	{
		options ??= CsvDataWriterOptions.Default;
		return new CsvDataWriter(writer, buffer, options);
	}

	CsvDataWriter(TextWriter writer, char[]? buffer, CsvDataWriterOptions? options = null)
	{
		options ??= CsvDataWriterOptions.Default;
		options.Validate();
		this.binaryEncoding = options.BinaryEncoding;
		this.writer = writer ?? throw new ArgumentNullException(nameof(writer));
		this.csvWriter = options.Style == CsvStyle.Standard ? CsvWriter.Quoted : CsvWriter.Escaped;
		this.trueString = options.TrueString;
		this.falseString = options.FalseString;
		this.dateTimeFormat = options.DateTimeFormat;
		this.dateTimeOffsetFormat = options.DateTimeOffsetFormat;
		this.timeSpanFormat = options.TimeSpanFormat;

#if NET6_0_OR_GREATER
		this.timeOnlyFormat = options.TimeOnlyFormat;
		this.dateOnlyFormat = options.DateOnlyFormat;
#endif
		this.writeHeaders = options.WriteHeaders;
		this.quoteEmpty = (options.QuoteStrings & CsvStringQuoting.AlwaysQuoteEmpty) != 0;
		this.quoteNonEmpty = (options.QuoteStrings & CsvStringQuoting.AlwaysQuoteNonEmpty) != 0;
		this.delimiter = options.Delimiter;
		this.quote = options.Quote;
		this.escape = options.Escape;
		this.comment = options.Comment;
		this.newLine = options.NewLine;
		this.culture = options.Culture;
#pragma warning disable CS0618 // Type or member is obsolete
		this.buffer = buffer ?? options.Buffer ?? new char[options.BufferSize];
#pragma warning restore CS0618 // Type or member is obsolete
		this.maxBufferSize = options.MaxBufferSize ?? -1;
		this.pos = 0;

		// create a lookup of all the characters that need to be escaped.
		this.needsEscape = GetEscapeFlags(options.Style);
	}

	bool[] GetEscapeFlags(CsvStyle style)
	{
		// avoid allocating flags for the default configuration
		var isDefault =
			delimiter == CsvDataWriterOptions.DefaultDelimiter &&
			quote == CsvDataWriterOptions.DefaultQuote &&
			escape == CsvDataWriterOptions.DefaultEscape &&
			style == CsvStyle.Standard;
		if (isDefault)
		{
			return DefaultEscapeFlags;
		}
		var flags = new bool[128];
		flags[delimiter] = true;
		flags[quote] = true;
		flags[escape] = true;
		flags['\r'] = true;
		flags['\n'] = true;

		if (style == CsvStyle.Escaped)
		{
			flags[comment] = true;
		}
		return flags;
	}

	static bool IsBase64Symbol(char c)
	{
		return c == '+' || c == '/' || c == '=';
	}
}
