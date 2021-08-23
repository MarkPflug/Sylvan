using System;
using System.Data.Common;

namespace Sylvan.Data.Csv
{
	partial class CsvDataWriter
	{
		interface IFieldWriter
		{
			int Write(WriterContext context, int ordinal, char[] buffer, int offset);
		}

		abstract class FieldWriter : IFieldWriter
		{
			public abstract int Write(WriterContext context, int ordinal, char[] buffer, int offset);
		}

		sealed class ObjectFieldWriter : FieldWriter
		{
			public static ObjectFieldWriter Instance = new ObjectFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var value = reader.GetValue(ordinal).ToString() ?? "";
				if (ordinal == 0 && value.Length > 0 && value[0] == context.writer.comment)
					return writer.csvWriter.WriteEscaped(context, value, buffer, offset);

				return writer.csvWriter.Write(context, value, buffer, offset);
			}
		}

		sealed class StringFieldWriter : FieldWriter
		{
			public static StringFieldWriter Instance = new StringFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var value = reader.GetString(ordinal);
				if (ordinal == 0 && value.Length > 0 && value[0] == context.writer.comment)
					return writer.csvWriter.WriteEscaped(context, value, buffer, offset);

				return writer.csvWriter.Write(context, value, buffer, offset);
			}
		}

		sealed class BinaryFieldWriter : FieldWriter
		{
			public static BinaryFieldWriter Instance = new BinaryFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;

				if (writer.dataBuffer.Length == 0)
				{
					writer.dataBuffer = new byte[Base64EncSize];
				}
				var dataBuffer = writer.dataBuffer;
				var idx = 0;

				int len;
				var pos = offset;
				while ((len = (int)reader.GetBytes(ordinal, idx, dataBuffer, 0, Base64EncSize)) != 0)
				{
					var req = (len + 2) / 3 * 4;
					if (pos + req >= buffer.Length)
						return InsufficientSpace;

					var c = Convert.ToBase64CharArray(dataBuffer, 0, len, buffer, pos);

					idx += len;
					pos += c;
				}
				return pos - offset;
			}
		}

		sealed class BooleanFieldWriter : FieldWriter
		{
			public static IFieldWriter Instance = new BooleanFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var w = context.writer;
				var culture = w.culture;

				var value = reader.GetBoolean(ordinal);
				var str = value ? w.trueString : w.falseString;
				return w.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class Int32FieldWriter : FieldWriter
		{
			public static Int32FieldWriter Instance = new Int32FieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetInt32(ordinal);
#if SPAN

				Span<char> str = stackalloc char[12];
				if (!value.TryFormat(str, out int len, default, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString(culture);
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class Int64FieldWriter : FieldWriter
		{
			public static Int64FieldWriter Instance = new Int64FieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetInt64(ordinal);
#if SPAN

				Span<char> str = stackalloc char[20];
				if (!value.TryFormat(str, out int len, default, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString(culture);
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class SingleFieldWriter : FieldWriter
		{
			public static SingleFieldWriter Instance = new SingleFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetFloat(ordinal);
#if SPAN

				Span<char> str = stackalloc char[14];
				if (!value.TryFormat(str, out int len, default, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString(culture);
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class DoubleFieldWriter : FieldWriter
		{
			public static DoubleFieldWriter Instance = new DoubleFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetDouble(ordinal);
#if SPAN

				Span<char> str = stackalloc char[16];
				if (!value.TryFormat(str, out int len, default, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString(culture);
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class DecimalFieldWriter : FieldWriter
		{
			public static DecimalFieldWriter Instance = new DecimalFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetDecimal(ordinal);
#if SPAN

				Span<char> str = stackalloc char[32];
				if (!value.TryFormat(str, out int len, default, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString(culture);
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class GuidFieldWriter : FieldWriter
		{
			public static GuidFieldWriter Instance = new GuidFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetGuid(ordinal);
#if SPAN

				Span<char> str = stackalloc char[36];
				if (!value.TryFormat(str, out int len, default))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString();
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class DateTimeFieldWriter : FieldWriter
		{
			public static DateTimeFieldWriter Instance = new DateTimeFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetDateTime(ordinal);
				var fmt = value.TimeOfDay == TimeSpan.Zero ? writer.dateFormat : writer.dateTimeFormat;
#if SPAN

				Span<char> str = stackalloc char[32];
				if (!value.TryFormat(str, out int len, fmt, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString(fmt, culture);
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class TimeSpanFieldWriter : FieldWriter
		{
			public static TimeSpanFieldWriter Instance = new TimeSpanFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetFieldValue<TimeSpan>(ordinal);
				var fmt = writer.timeSpanFormat;
#if SPAN

				Span<char> str = stackalloc char[32];
				if (!value.TryFormat(str, out int len, fmt, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);

#else
				var str = value.ToString(fmt, culture);
#endif
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}


#if SPAN

		sealed class Int32FastFieldWriter : FieldWriter
		{
			public static Int32FastFieldWriter Instance = new Int32FastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetInt32(ordinal);
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, default, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class Int64FastFieldWriter : FieldWriter
		{
			public static Int64FastFieldWriter Instance = new Int64FastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetInt64(ordinal);
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, default, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class DateTimeFastFieldWriter : FieldWriter
		{
			public static DateTimeFastFieldWriter Instance = new DateTimeFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetDateTime(ordinal);
				var fmt = value.TimeOfDay == TimeSpan.Zero ? writer.dateFormat : writer.dateTimeFormat;
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, fmt, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class TimeSpanFastFieldWriter : FieldWriter
		{
			public static TimeSpanFastFieldWriter Instance = new TimeSpanFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetFieldValue<TimeSpan>(ordinal);
				var fmt = writer.timeSpanFormat;
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, fmt, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

#if NET6_0_OR_GREATER

		sealed class DateOnlyFastFieldWriter : FieldWriter
		{
			public static DateOnlyFastFieldWriter Instance = new DateOnlyFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetFieldValue<DateOnly>(ordinal);
				var fmt = writer.dateFormat;
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, fmt, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class DateOnlyFieldWriter : FieldWriter
		{
			public static DateOnlyFieldWriter Instance = new DateOnlyFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetFieldValue<DateOnly>(ordinal);
				var fmt = writer.dateFormat;
				Span<char> str = stackalloc char[32];
				if (!value.TryFormat(str, out int len, fmt, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				str = str.Slice(0, len);
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
		}

		sealed class TimeOnlyFastFieldWriter : FieldWriter
		{
			public static TimeOnlyFastFieldWriter Instance = new TimeOnlyFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetFieldValue<TimeOnly>(ordinal);
				var fmt = writer.timeFormat;
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, fmt, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class TimeOnlyFieldWriter : FieldWriter
		{
			public static TimeOnlyFieldWriter Instance = new TimeOnlyFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetDateTime(ordinal);
				var fmt = writer.timeFormat;
				Span<char> span = stackalloc char[32];
				if (!value.TryFormat(span, out int len, fmt, culture))
				{
					throw new FormatException(); // this shouldn't happen
				}

				span = span.Slice(0, len);
				return writer.csvWriter.Write(context, span, buffer, offset);
			}
		}

#endif

		sealed class SingleFastFieldWriter : FieldWriter
		{
			public static SingleFastFieldWriter Instance = new SingleFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetFloat(ordinal);
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, default, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class DoubleFastFieldWriter : FieldWriter
		{
			public static DoubleFastFieldWriter Instance = new DoubleFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetDouble(ordinal);
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, default, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class DecimalFastFieldWriter : FieldWriter
		{
			public static DecimalFastFieldWriter Instance = new DecimalFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetDecimal(ordinal);
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, default, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}

		sealed class GuidFastFieldWriter : FieldWriter
		{
			public static GuidFastFieldWriter Instance = new GuidFastFieldWriter();

			public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
			{
				var reader = context.reader;
				var writer = context.writer;
				var culture = writer.culture;
				var value = reader.GetGuid(ordinal);
				var span = buffer.AsSpan(offset);
				if (!value.TryFormat(span, out int len, default))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}
#endif

	}
}
