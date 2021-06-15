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
				var span = buffer.AsSpan().Slice(offset);
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
				var span = buffer.AsSpan().Slice(offset);
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
				var span = buffer.AsSpan().Slice(offset);
				if (!value.TryFormat(span, out int len, default, culture))
				{
					return InsufficientSpace;
				}
				return len;
			}
		}
#endif

	}
}
