using System;
using System.Collections.Concurrent;
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
	const int InsufficientSpace = -1;
	const int NeedsQuoting = -2;

	class FieldInfo
	{
		internal static readonly FieldInfo Generic = new FieldInfo(true, ObjectFieldWriter.Instance);

		public FieldInfo(bool allowNull, IFieldWriter writer)
		{
			this.allowNull = allowNull;
			this.writer = writer;
		}

		public bool allowNull;
		public IFieldWriter writer;
	}

	IFieldWriter GetWriter(DbDataReader reader, int ordinal)
	{
		var type = reader.GetFieldType(ordinal);

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
			var fmt = this.dateTimeFormat;
#if SPAN
			if (IsFastDateTime)
			{
				return fmt == null
					? DateTimeIsoFastFieldWriter.Instance
					: DateTimeFormatFastFieldWriter.Instance;
			}
			else
			{
				return fmt == null
					? DateTimeIsoFieldWriter.Instance
					: DateTimeFormatFieldWriter.Instance;
			}
#else
			return DateTimeFormatFieldWriter.Instance;
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
			return IsFastDate
				? DateOnlyIsoFastFieldWriter.Instance
				: DateOnlyIsoFieldWriter.Instance;
		}

		if (type == typeof(TimeOnly))
		{
			return IsFastTime
				? TimeOnlyFastFieldWriter.Instance
				: TimeOnlyFieldWriter.Instance;
		}

#endif

#if SPAN
		if (type.IsEnum)
		{
			IFieldWriter? writer;
			if (!enumMap.TryGetValue(type, out writer))
			{
				if (IsCandidateEnum(type))
				{
					// multiple calls to MakeGenericType return the same instance.
					var prop = typeof(EnumFastFieldWriter<>).MakeGenericType(type).GetField("Instance", BindingFlags.Static | BindingFlags.Public)!;
					writer = (IFieldWriter)prop.GetValue(null)!;
				}
				else
				{
					writer = ObjectFieldWriter.Instance;
				}
				enumMap.TryAdd(type, writer);
			}

			return writer;
		}

#endif

		// for everything else fallback to GetValue/ToString
		return ObjectFieldWriter.Instance;
	}

#if SPAN

	static ConcurrentDictionary<Type, IFieldWriter> enumMap = new ConcurrentDictionary<Type, IFieldWriter>();

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
				&& this.dateTimeFormat == CsvDataWriterOptions.Default.DateTimeFormat;
		}
	}

	bool IsFastDate
	{
		get
		{
			return IsInvariantCulture && IsFastConfig
				&& this.dateFormat == CsvDataWriterOptions.Default.DateFormat;
		}
	}

	bool IsFastTimeSpan
	{
		get
		{
			return IsInvariantCulture && IsFastConfig
				&& this.timeSpanFormat == CsvDataWriterOptions.Default.TimeSpanFormat;
		}
	}

#if NET6_0_OR_GREATER
	bool IsFastTime
	{
		get
		{
			return IsInvariantCulture && IsFastConfig
				&& this.timeFormat == CsvDataWriterOptions.Default.TimeFormat;
		}
	}
#endif

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
	readonly BinaryEncoding binaryEncoding;

	readonly string trueString;
	readonly string falseString;
	readonly string? dateTimeFormat;
	readonly string? dateTimeOffsetFormat;
	readonly string? timeSpanFormat;
	readonly string? dateFormat;
#if NET6_0_OR_GREATER
	readonly string? timeFormat;
#endif

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
		return new CsvDataWriter(writer, null, options);
	}

	/// <summary>
	/// Creates a new CsvDataWriter.
	/// </summary>
	/// <param name="writer">The TextWriter to receive the delimited data.</param>
	/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
	public static CsvDataWriter Create(TextWriter writer, CsvDataWriterOptions? options = null)
	{
		options = options ?? CsvDataWriterOptions.Default;
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
		options = options ?? CsvDataWriterOptions.Default;
		return new CsvDataWriter(writer, buffer, options);
	}

	CsvDataWriter(TextWriter writer, char[]? buffer, CsvDataWriterOptions? options = null)
	{
		options = options ?? CsvDataWriterOptions.Default;
		options.Validate();
		this.binaryEncoding = options.BinaryEncoding;
		this.writer = writer ?? throw new ArgumentNullException(nameof(writer));
		this.csvWriter = options.Style == CsvStyle.Standard ? CsvWriter.Quoted : CsvWriter.Escaped;
		this.trueString = options.TrueString;
		this.falseString = options.FalseString;
		this.dateTimeFormat = options.DateTimeFormat;
		this.dateTimeOffsetFormat = options.DateTimeOffsetFormat;
		this.timeSpanFormat = options.TimeSpanFormat;
		this.dateFormat = options.DateFormat;
#if NET6_0_OR_GREATER
		this.timeFormat = options.TimeFormat;
#endif
		this.writeHeaders = options.WriteHeaders;
		this.delimiter = options.Delimiter;
		this.quote = options.Quote;
		this.escape = options.Escape;
		this.comment = options.Comment;
		this.newLine = options.NewLine;
		this.culture = options.Culture;
#pragma warning disable CS0618 // Type or member is obsolete
		this.buffer = buffer ?? options.Buffer ?? new char[options.BufferSize];
#pragma warning restore CS0618 // Type or member is obsolete
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
