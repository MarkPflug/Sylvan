using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Sylvan.Data.Csv;

partial class CsvDataWriter
{
	abstract class FieldWriter
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

	sealed class BinaryBase64FieldWriter : FieldWriter
	{
		public static BinaryBase64FieldWriter Instance = new BinaryBase64FieldWriter();

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

	sealed class BinaryHexFieldWriter : FieldWriter
	{
		static char[] HexMap = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		public static BinaryHexFieldWriter Instance = new BinaryHexFieldWriter();

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
				var req = len * 2;
				if (pos + req >= buffer.Length)
				{
					// force a flush
					return InsufficientSpace;
				}

				var c = ToHexCharArray(dataBuffer, 0, len, buffer, pos);

				idx += len;
				pos += c;
			}
			return pos - offset;
		}

		static int ToHexCharArray(byte[] dataBuffer, int offset, int length, char[] outputBuffer, int outputOffset)
		{
			if (length * 2 > outputBuffer.Length - outputOffset)
				throw new ArgumentException();

			var idx = offset;
			var end = offset + length;
			for (; idx < end; idx++)
			{
				var b = dataBuffer[idx];
				var lo = HexMap[b & 0xf];
				var hi = HexMap[b >> 4];
				outputBuffer[outputOffset++] = hi;
				outputBuffer[outputOffset++] = lo;
			}
			return length * 2;
		}
	}

	sealed class BooleanFieldWriter : FieldWriter
	{
		public static FieldWriter Instance = new BooleanFieldWriter();

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

#if SPAN
	sealed class DateTimeIsoFieldWriter : FieldWriter
	{
		public static DateTimeIsoFieldWriter Instance = new DateTimeIsoFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;
			var culture = writer.culture;
			var value = reader.GetDateTime(ordinal);

			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			int len;
			if (!IsoDate.TryFormatIso(value, str, out len))
			{
				return InsufficientSpace;
			}
			str = str.Slice(0, len);
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}
#endif

	sealed class DateTimeFormatFieldWriter : FieldWriter
	{
		public static DateTimeFormatFieldWriter Instance = new DateTimeFormatFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;
			var culture = writer.culture;
			var value = reader.GetDateTime(ordinal);
			var fmt = writer.dateTimeFormat ?? "O";
#if SPAN
			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			int len;

			if (!value.TryFormat(str, out len, fmt, culture))
			{
				return InsufficientSpace;
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

	sealed class DateTimeFormatFastFieldWriter : FieldWriter
	{
		public static DateTimeFormatFastFieldWriter Instance = new DateTimeFormatFastFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;
			var culture = writer.culture;
			var value = reader.GetDateTime(ordinal);
			var fmt = writer.dateTimeFormat;
			var span = buffer.AsSpan(offset);
			return
				value.TryFormat(span, out int len, fmt, culture)
				? len
				: InsufficientSpace;
		}
	}

	sealed class DateTimeIsoFastFieldWriter : FieldWriter
	{
		public static DateTimeIsoFastFieldWriter Instance = new DateTimeIsoFastFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var value = reader.GetDateTime(ordinal);
			var span = buffer.AsSpan(offset);
			return
				IsoDate.TryFormatIso(value, span, out int len)
				? len
				: InsufficientSpace;
		}
	}

	sealed class DateTimeOffsetFormatFastFieldWriter : FieldWriter
	{
		public static DateTimeOffsetFormatFastFieldWriter Instance = new DateTimeOffsetFormatFastFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;
			var culture = writer.culture;
			var value = reader.GetFieldValue<DateTimeOffset>(ordinal);
			var fmt = writer.dateTimeOffsetFormat;
			var span = buffer.AsSpan(offset);
			return
				value.TryFormat(span, out int len, fmt, culture)
				? len
				: InsufficientSpace;
		}
	}

	sealed class DateTimeOffsetIsoFastFieldWriter : FieldWriter
	{
		public static DateTimeOffsetIsoFastFieldWriter Instance = new DateTimeOffsetIsoFastFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var value = reader.GetFieldValue<DateTimeOffset>(ordinal);
			var span = buffer.AsSpan(offset);
			return
				IsoDate.TryFormatIso(value, span, out int len)
				? len
				: InsufficientSpace;
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

	sealed class DateOnlyFormatFastFieldWriter : FieldWriter
	{
		public static DateOnlyFormatFastFieldWriter Instance = new DateOnlyFormatFastFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;
			var culture = writer.culture;
			var value = reader.GetFieldValue<DateOnly>(ordinal);
			var fmt = writer.dateOnlyFormat;
			var span = buffer.AsSpan(offset);
			int len;
			return
				value.TryFormat(span, out len, fmt, culture)
				? len
				: InsufficientSpace;
		}
	}

	sealed class DateOnlyIsoFastFieldWriter : FieldWriter
	{
		public static DateOnlyIsoFastFieldWriter Instance = new DateOnlyIsoFastFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var value = reader.GetFieldValue<DateOnly>(ordinal);
			var span = buffer.AsSpan(offset);
			int len;
			return IsoDate.TryFormatIso(value, span, out len)
				? len
				: InsufficientSpace;
		}
	}

	sealed class DateOnlyFormatFieldWriter : FieldWriter
	{
		public static DateOnlyFormatFieldWriter Instance = new DateOnlyFormatFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;
			var culture = writer.culture;
			var value = reader.GetFieldValue<DateOnly>(ordinal);
			var fmt = writer.dateOnlyFormat;
			int len;
			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (!value.TryFormat(str, out len, fmt, culture))
			{
				return InsufficientSpace;
			}
			str = str.Slice(0, len);
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class DateOnlyIsoFieldWriter : FieldWriter
	{
		public static DateOnlyIsoFieldWriter Instance = new DateOnlyIsoFieldWriter();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;
			var culture = writer.culture;
			var value = reader.GetFieldValue<DateOnly>(ordinal);
			int len;
			Span<char> str = stackalloc char[IsoDate.MaxDateOnlyLength];
			if (!IsoDate.TryFormatIso(value, str, out len))
			{
				return InsufficientSpace;
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
			var fmt = writer.timeOnlyFormat;
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
			var fmt = writer.timeOnlyFormat;
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

	sealed class EnumFastFieldWriter<T> : FieldWriter where T : Enum
	{
		public static EnumFastFieldWriter<T> Instance = new EnumFastFieldWriter<T>();

		readonly string[] values;

		readonly Func<EnumFastFieldWriter<T>, T, string> func;

		private EnumFastFieldWriter()
		{
			var values = Enum.GetValues(typeof(T));
			var map = new Dictionary<int, string>();
			foreach (var value in values)
			{
				var v = Convert.ToInt32(value);
				map.Add(v, value!.ToString()!);
			}
			this.values = new string[map.Keys.Max() + 1];
			foreach (var kvp in map)
			{
				this.values[kvp.Key] = kvp.Value;
			}

			var thisParam = Expression.Parameter(typeof(EnumFastFieldWriter<T>), "this");
			var valueParam = Expression.Parameter(typeof(T), "value");

			var body =
				Expression.Condition(
					Expression.LessThan(
						Expression.Convert(valueParam, typeof(uint)),
						Expression.Constant((uint)this.values.Length)
					),
					Expression.ArrayAccess(
						Expression.Field(thisParam, nameof(values)),
						Expression.Convert(valueParam, typeof(int))
					),
					Expression.Constant(null, typeof(string))
				);

			var lambda = Expression.Lambda<Func<EnumFastFieldWriter<T>, T, string>>(body, thisParam, valueParam);
			this.func = lambda.Compile();
		}

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var value = reader.GetFieldValue<T>(ordinal);
			var span = buffer.AsSpan(offset);
			var str = func(this, value) ?? value.ToString();
			var len = str.Length;
			if (len <= span.Length)
			{
				str.AsSpan().CopyTo(span);
				return len;
			}
			return InsufficientSpace;
		}
	}

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
