using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
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

		interface IFieldWriter
		{
			int Write(WriterContext context, int ordinal, char[] buffer, int offset);
		}


		class WriterContext
		{
			internal CsvDataWriter writer;
			internal DbDataReader reader;

			public WriterContext(CsvDataWriter writer, DbDataReader reader)
			{
				this.writer = writer;
				this.reader = reader;
			}
		}

		sealed class DateTimeFastFieldWriter : IFieldWriter
		{
			public static IFieldWriter Instance = new DateTimeFastFieldWriter();

			public int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
#if SPAN
				var reader = context.reader;
				var w = context.writer;
				var culture = w.culture;

				var value = reader.GetDateTime(ordinal);
				var format = value.TimeOfDay == TimeSpan.Zero ? w.dateFormat : w.dateTimeFormat;

				if (value.TryFormat(buffer.AsSpan().Slice(offset), out int len, format, culture))
				{
					return len;
				}
				return -1;
#else
				return ObjectFieldWriter.Instance.Write(context, ordinal, buffer, offset);
#endif
			}
		}

		sealed class BooleanFieldWriter : IFieldWriter
		{
			public static IFieldWriter Instance = new BooleanFieldWriter();

			public int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
#if SPAN
				var reader = context.reader;
				var w = context.writer;
				var culture = w.culture;

				var value = reader.GetBoolean(ordinal);
				var str = value ? w.trueString : w.falseString;
				var span = str.AsSpan();
				return CsvDataWriter.Write(context, span, buffer.AsSpan().Slice(offset));
#else
				return ObjectFieldWriter.Instance.Write(context, ordinal, buffer, offset);
#endif
			}
		}

		sealed class Int32FastFieldWriter : IFieldWriter
		{
			public static IFieldWriter Instance = new Int32FastFieldWriter();

			public int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
#if SPAN
				var reader = context.reader;
				var culture = context.writer.culture;

				var value = reader.GetInt32(ordinal);
				if (value.TryFormat(buffer.AsSpan().Slice(offset), out int len, default, culture))
				{
					return len;
				}
				return -1;
#else
				return ObjectFieldWriter.Instance.Write(context, ordinal, buffer, offset);
#endif
			}
		}

		sealed class Int32FieldWriter : IFieldWriter
		{
			public static IFieldWriter Instance = new Int32FastFieldWriter();

			public int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
#if SPAN
				var reader = context.reader;
				var culture = context.writer.culture;

				Span<char> span = stackalloc char[16];

				var value = reader.GetInt32(ordinal);
				if (!value.TryFormat(span, out int len, default, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				var src = span.Slice(0, len);
				var dst = buffer.AsSpan().Slice(offset);
				return CsvDataWriter.Write(context, src, dst);
#else
				return ObjectFieldWriter.Instance.Write(context, ordinal, buffer, offset);
#endif
			}
		}

		sealed class DoubleFastFieldWriter : IFieldWriter
		{
			public static IFieldWriter Instance = new DoubleFastFieldWriter();

			public int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
#if SPAN
				var reader = context.reader;
				var culture = context.writer.culture;

				var value = reader.GetDouble(ordinal);
				if (value.TryFormat(buffer.AsSpan().Slice(offset), out int len, default, culture))
				{
					return len;
				}
				return -1;
#else
				return ObjectFieldWriter.Instance.Write(context, ordinal, buffer, offset);
#endif
			}
		}

		sealed class ObjectFieldWriter : IFieldWriter
		{
			public static IFieldWriter Instance = new ObjectFieldWriter();

			public int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var value = reader.GetValue(ordinal).ToString() ?? "";
				if (ordinal == 0 && value.Length > 0 && value[0] == context.writer.comment)
					return WriteQuoted(context, value, buffer, offset);

				return CsvDataWriter.Write(context, value, buffer, offset);
			}
		}

		class FieldInfo
		{
			public FieldInfo(bool allowNull, Type type)
			{
				this.allowNull = allowNull;
				this.type = type;
				this.typeCode = Type.GetTypeCode(type);
				this.writer = GetFieldWriter(type);
			}

			public bool allowNull;
			public Type type;
			public TypeCode typeCode;
			public IFieldWriter writer;

			static IFieldWriter GetFieldWriter(Type t)
			{
				if (t == typeof(int))
					return Int32FastFieldWriter.Instance;
				if (t == typeof(double))
					return DoubleFastFieldWriter.Instance;
				if(t == typeof(DateTime))
					return DateTimeFastFieldWriter.Instance;
				if(t == typeof(bool))
					return BooleanFieldWriter.Instance;
				return ObjectFieldWriter.Instance;
			}

			//static readonly Dictionary<Type, IFieldWriter> FieldWriters;

			//static FieldInfo()
			//{
			//	FieldWriters = new Dictionary<Type, IFieldWriter>();
			//	FieldWriters.Add(typeof(int), int32)

			//}
		}

		enum WriteResult
		{
			InsufficientSpace = 1,
			RequiresEscaping,
			Complete,
		}

		// Size of the buffer used for base64 encoding, must be a multiple of 3.
		const int Base64EncSize = 3 * 256;

		readonly TextWriter writer;
		readonly CsvStyle style;
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

		readonly bool fastDouble;
		readonly bool fastInt;
		readonly bool fastDate;
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
			this.style = options.Style;
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
			//Flag(comment);
			Flag('\r');
			Flag('\n');

			var isInvariantCulture = this.culture == CultureInfo.InvariantCulture;
			this.fastDouble = isInvariantCulture && delimiter != '.' && quote != '.';
			this.fastInt = isInvariantCulture && delimiter != '-' && quote != '-';
			this.fastDate = isInvariantCulture && delimiter == ',' && quote == '\"';
		}

		void Flag(char c)
		{
			// these characters are already validated to be in 0-127
			needsEscape[c] = true;
		}

		// this can return either Complete or InsufficientSpace
		//		WriteResult WriteField(DbDataReader reader, FieldInfo[] fieldTypes, int i)
		//		{
		//			var allowNull = fieldTypes[i].allowNull;

		//			if (allowNull && reader.IsDBNull(i))
		//			{
		//				return WriteResult.Complete;
		//			}

		//			int intVal;
		//			string? str;
		//			WriteResult result = WriteResult.Complete;

		//			var typeCode = fieldTypes[i].typeCode;
		//			switch (typeCode)
		//			{
		//				case TypeCode.Boolean:
		//					var boolVal = reader.GetBoolean(i);
		//					result = WriteField(boolVal);
		//					break;
		//				case TypeCode.String:
		//					str = reader.GetString(i);
		//					if (i == 0 && str.Length > 0 && str[0] == comment)
		//					{
		//						if (style == CsvStyle.Standard)
		//						{
		//							result = WriteQuoted(str);
		//						}
		//						else
		//						{
		//							result = WriteEscapedValue(str);
		//						}
		//					}
		//					else
		//					{
		//						result = WriteField(str);
		//					}
		//					break;
		//				case TypeCode.Byte:
		//					intVal = reader.GetByte(i);
		//					goto intVal;
		//				case TypeCode.Int16:
		//					intVal = reader.GetInt16(i);
		//					goto intVal;
		//				case TypeCode.Int32:
		//					intVal = reader.GetInt32(i);
		//					intVal:
		//					result = WriteField(intVal);
		//					break;
		//				case TypeCode.Int64:
		//					var longVal = reader.GetInt64(i);
		//					result = WriteField(longVal);
		//					break;
		//				case TypeCode.DateTime:
		//					var dateVal = reader.GetDateTime(i);
		//					result = WriteField(dateVal);
		//					break;
		//				case TypeCode.Single:
		//					var floatVal = reader.GetFloat(i);
		//					result = WriteField(floatVal);
		//					break;
		//				case TypeCode.Double:
		//					var doubleVal = reader.GetDouble(i);
		//					result = WriteField(doubleVal);
		//					break;
		//				case TypeCode.Empty:
		//				case TypeCode.DBNull:
		//					// nothing to do.
		//					result = WriteResult.Complete;
		//					break;
		//				default:
		//					var type = fieldTypes[i].type;
		//					if (type == typeof(byte[]))
		//					{
		//						if (dataBuffer.Length == 0)
		//						{
		//							dataBuffer = new byte[Base64EncSize];
		//						}
		//						var idx = 0;
		//						if (IsBase64Symbol(delimiter) || IsBase64Symbol(quote))
		//						{
		//							throw new InvalidOperationException();
		//						}
		//						int len;
		//						var pos = this.pos;
		//						while ((len = (int)reader.GetBytes(i, idx, dataBuffer, 0, Base64EncSize)) != 0)
		//						{
		//							var req = (len + 2) / 3 * 4;
		//							if (pos + req >= buffer.Length)
		//								return WriteResult.InsufficientSpace;

		//							var c = Convert.ToBase64CharArray(dataBuffer, 0, len, this.buffer, pos);

		//							idx += len;
		//							pos += c;
		//						}
		//						this.pos = pos;
		//						result = WriteResult.Complete;
		//						break;
		//					}
		//					if (type == typeof(Guid))
		//					{
		//						var guid = reader.GetGuid(i);
		//						result = WriteField(guid);
		//						break;
		//					}

		//					if (type == typeof(TimeSpan))
		//					{
		//						var ts = reader.GetFieldValue<TimeSpan>(i);
		//						result = WriteField(ts);
		//						break;
		//					}
		//#if NET6_0_OR_GREATER
		//					if (type == typeof(DateOnly))
		//					{
		//						var d = reader.GetFieldValue<DateOnly>(i);
		//						result = WriteField(d);
		//						break;
		//					}

		//					if (type == typeof(TimeOnly))
		//					{
		//						var t = reader.GetFieldValue<TimeOnly>(i);
		//						result = WriteField(t);
		//						break;
		//					}
		//#endif
		//					str = reader.GetValue(i)?.ToString() ?? string.Empty;
		//					result = WriteField(str);
		//					break;
		//			}
		//			return result;
		//		}

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

		WriteResult WriteField(Guid value)
		{
			return WriteField(value.ToString());
		}

		WriteResult WriteField(bool value)
		{
			var str = value ? trueString : falseString;
			return WriteField(str);
		}

		WriteResult WriteField(double value)
		{
#if SPAN
			if (fastDouble)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
#endif
			return WriteField(value.ToString(culture));
		}

		WriteResult WriteField(long value)
		{
#if SPAN
			if (fastInt)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
#endif
			return WriteField(value.ToString(culture));
		}

		WriteResult WriteField(int value)
		{
#if SPAN
			if (fastInt)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
#endif
			return WriteField(value.ToString(culture));
		}

		WriteResult WriteField(DateTime value)
		{
			var format = value.TimeOfDay == TimeSpan.Zero ? dateFormat : dateTimeFormat;
#if SPAN
			if (fastDate)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, format, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
#endif
			return WriteField(value.ToString(format, culture));
		}

		WriteResult WriteField(TimeSpan value)
		{
#if SPAN
			if (fastDate)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
#endif
			return WriteField(value.ToString(null, culture));
		}

#if NET6_0_OR_GREATER
		WriteResult WriteField(DateOnly value)
		{
			var format = dateFormat;

			if (fastDate)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, format, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
			return WriteField(value.ToString(format, culture));
		}

		WriteResult WriteField(TimeOnly value)
		{
			if (fastDate)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
			return WriteField(value.ToString(null, culture));
		}
#endif

		WriteResult WriteField(string value)
		{
			if (style == CsvStyle.Standard)
			{
				var r = WriteValueOptimistic(value);
				if (r == WriteResult.RequiresEscaping)
				{
					return WriteQuoted(value);
				}
				return r;
			}
			else
			{
				return WriteEscapedValue(value);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		WriteResult WriteValueOptimistic(string str)
		{
			var buffer = this.buffer;
			var pos = this.pos;
			if (pos + str.Length >= buffer.Length)
				return WriteResult.InsufficientSpace;

			for (int i = 0; i < str.Length; i++)
			{
				var c = str[i];
				if (c < needsEscape.Length && !needsEscape[c])
				{
					buffer[pos + i] = c;
				}
				else
				{
					if (c == delimiter || c == quote || c == '\n' || c == '\r')
					{
						return WriteResult.RequiresEscaping;
					}
					buffer[pos + i] = c;
				}
			}
			this.pos += str.Length;
			return WriteResult.Complete;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		WriteResult WriteEscapedValue(string str)
		{
			var buffer = this.buffer;
			var pos = this.pos;
			// for simplicity assume every character will need to be escaped
			if (pos + str.Length * 2 >= buffer.Length)
				return WriteResult.InsufficientSpace;

			for (int i = 0; i < str.Length; i++)
			{
				char c = str[i];
				if (c < needsEscape.Length && !needsEscape[c])
				{
					buffer[pos++] = c;
				}
				else
				{
					if (c == '\r')
					{
						buffer[pos++] = escape;
						buffer[pos++] = c;
						if (str[i + 1] == '\n')
						{
							buffer[pos++] = '\n';
							i++;
						}
					}
					else
					{
						buffer[pos++] = escape;
						buffer[pos++] = c;
					}
				}
			}
			this.pos = pos;
			return WriteResult.Complete;
		}

		WriteResult WriteQuoted(string str)
		{
			var buffer = this.buffer;
			var p = this.pos;
			// require at least room for the 2 quotes and 1 escape.
			if (p + str.Length + 3 >= buffer.Length)
				return WriteResult.InsufficientSpace;

			buffer[p++] = quote; // range guarded by previous if
			for (int i = 0; i < str.Length; i++)
			{
				var c = str[i];

				if (c == quote || c == escape)
				{
					if (p == buffer.Length)
						return WriteResult.InsufficientSpace;
					buffer[p++] = escape;
				}

				if (p == buffer.Length)
					return WriteResult.InsufficientSpace;
				buffer[p++] = c;
			}
			if (p == buffer.Length)
				return WriteResult.InsufficientSpace;
			buffer[p++] = quote;
			this.pos = p;
			return WriteResult.Complete;
		}


		const int InsufficientSpace = -1;
		const int NeedsQuoting = -2;

#if SPAN

		static int Write(WriterContext context, ReadOnlySpan<char> src, Span<char> dst)
		{
			var r = WriteOptimistic(context, src, dst);
			return
				r == NeedsQuoting
				? WriteQuoted(context, src, dst)
				: r;
		}

		static int WriteOptimistic(WriterContext context, ReadOnlySpan<char> src, Span<char> dst)
		{
			var ns = context.writer.needsEscape;
			var delimiter = context.writer.delimiter;
			var quote = context.writer.quote;

			if (src.Length > dst.Length)
				return InsufficientSpace;

			for (int i = 0; i < src.Length; i++)
			{
				var c = src[i];
				if (c >= ns.Length || !ns[c])
				{
					dst[i] = c;
				}
				else
				{
					return NeedsQuoting;
				}
			}
			return src.Length;
		}

		static int WriteQuoted(WriterContext context, ReadOnlySpan<char> src, Span<char> dst)
		{
			var quote = context.writer.quote;
			var escape = context.writer.escape;
			int p = 0;
			// require at least room for the 2 quotes and 1 escape.
			if (src.Length + 3 > dst.Length)
				return -1;

			dst[p++] = quote; // range guarded by previous if
			for (int i = 0; i < src.Length; i++)
			{
				var c = src[i];

				if (c == quote || c == escape)
				{
					if (p == dst.Length)
						return -1;
					dst[p++] = escape;
				}

				if (p == dst.Length)
					return -1;
				dst[p++] = c;
			}
			if (p == dst.Length)
				return -1;
			dst[p++] = quote;
			return p;
		}

#endif

		static int Write(WriterContext context, string value, char[] buffer, int offset)
		{
			var r = WriteOptimistic(context, value, buffer, offset);
			return r == -2
				? WriteQuoted(context, value, buffer, offset)
				: r;
		}

		static int WriteOptimistic(WriterContext context, string value, char[] buffer, int offset)
		{
			var pos = offset;
			var ns = context.writer.needsEscape;
			var delimiter = context.writer.delimiter;
			var quote = context.writer.quote;

			if (pos + value.Length >= buffer.Length)
				return -1;

			for (int i = 0; i < value.Length; i++)
			{
				var c = value[i];
				if (c >= ns.Length || !ns[c])
				{
					buffer[pos + i] = c;
				}
				else
				{
					return -2;
				}
			}
			return value.Length;
		}

		static int WriteQuoted(WriterContext context, string value, char[] buffer, int offset)
		{
			var p = offset;
			var quote = context.writer.quote;
			var escape = context.writer.escape;

			// require at least room for the 2 quotes and 1 escape.
			if (p + value.Length + 3 >= buffer.Length)
				return -1;

			buffer[p++] = quote; // range guarded by previous if
			for (int i = 0; i < value.Length; i++)
			{
				var c = value[i];

				if (c == quote || c == escape)
				{
					if (p == buffer.Length)
						return -1;
					buffer[p++] = escape;
				}

				if (p == buffer.Length)
					return -1;
				buffer[p++] = c;
			}
			if (p == buffer.Length)
				return -1;
			buffer[p++] = quote;
			return p - offset;
		}
	}
}
