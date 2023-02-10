using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;

namespace Sylvan.Data.Csv;

partial class CsvDataWriter
{
	// "Fast" writers can be used in scenarios where we know it is safe to write directly
	// to the output buffer. This is possible when the CSV config doesn't use any odd characters
	// and the format is default, and the invariant culture is used. Otherwise, quoting/escaping
	// and allocation might be necessary
	abstract class FieldWriter
	{
		public abstract int Write(WriterContext context, int ordinal, char[] buffer, int offset);
	}

	abstract class FieldWriter<T> : FieldWriter
	{
		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			T value = GetValue(context.reader, ordinal);
			return WriteValue(context, value, buffer, offset);
		}

		public abstract T GetValue(DbDataReader reader, int ordinal);

		public abstract int WriteValue(WriterContext context, T value, char[] buffer, int offset);
	}

	sealed class ObjectFieldWriter : FieldWriter<object>
	{
		public ObjectFieldWriter(CsvDataWriter writer)
		{
			this.booleanWriter = (FieldWriter<bool>)writer.GetWriter(typeof(bool));
			this.stringWriter = (FieldWriter<string>)writer.GetWriter(typeof(string));
			this.int32Writer = (FieldWriter<int>)writer.GetWriter(typeof(int));
			this.int64Writer = (FieldWriter<long>)writer.GetWriter(typeof(long));
			this.dateTimeWriter = (FieldWriter<DateTime>)writer.GetWriter(typeof(DateTime));
			this.dateTimeOffsetWriter = (FieldWriter<DateTimeOffset>)writer.GetWriter(typeof(DateTimeOffset));
			this.float32Writer = (FieldWriter<float>)writer.GetWriter(typeof(float));
			this.float64Writer = (FieldWriter<double>)writer.GetWriter(typeof(double));
			this.decimalWriter = (FieldWriter<decimal>)writer.GetWriter(typeof(decimal));
			this.guidWriter = (FieldWriter<Guid>)writer.GetWriter(typeof(Guid));
			this.binaryWriter = (FieldWriter<byte[]>)writer.GetWriter(typeof(byte[]));

#if NET6_0_OR_GREATER
			this.dateOnlyWriter= (FieldWriter<DateOnly>)writer.GetWriter(typeof(DateOnly));
			this.timeOnlyWriter = (FieldWriter<TimeOnly>)writer.GetWriter(typeof(TimeOnly));
#endif
		}

		readonly FieldWriter<bool> booleanWriter;
		readonly FieldWriter<string> stringWriter;
		readonly FieldWriter<int> int32Writer;
		readonly FieldWriter<long> int64Writer;
		readonly FieldWriter<float> float32Writer;
		readonly FieldWriter<double> float64Writer;
		readonly FieldWriter<decimal> decimalWriter;
		readonly FieldWriter<Guid> guidWriter;
		readonly FieldWriter<byte[]> binaryWriter;
		readonly FieldWriter<DateTime> dateTimeWriter;
		readonly FieldWriter<DateTimeOffset> dateTimeOffsetWriter;

#if NET6_0_OR_GREATER
		readonly FieldWriter<DateOnly> dateOnlyWriter;
		readonly FieldWriter<TimeOnly> timeOnlyWriter;
#endif
		public override object GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetValue(ordinal);
		}

		public override int WriteValue(WriterContext context, object value, char[] buffer, int offset)
		{
			var t = value.GetType();
			var c = Type.GetTypeCode(t);

			switch (c)
			{
				case TypeCode.DBNull:
					return stringWriter.WriteValue(context, string.Empty, buffer, offset);
				case TypeCode.String:
					return stringWriter.WriteValue(context, (string)value, buffer, offset);
				case TypeCode.Byte:
					return int32Writer.WriteValue(context, (byte)value, buffer, offset);
				case TypeCode.Int16:
					return int32Writer.WriteValue(context, (short)value, buffer, offset);
				case TypeCode.Int32:
					return int32Writer.WriteValue(context, (int)value, buffer, offset);
				case TypeCode.Int64:
					return int64Writer.WriteValue(context, (int)value, buffer, offset);
				case TypeCode.Single:
					return float32Writer.WriteValue(context, (float)value, buffer, offset);
				case TypeCode.Double:
					return float64Writer.WriteValue(context, (double)value, buffer, offset);
				case TypeCode.DateTime:
					return dateTimeWriter.WriteValue(context, (DateTime)value, buffer, offset);
				case TypeCode.Boolean:
					return booleanWriter.WriteValue(context, (bool)value, buffer, offset);
				case TypeCode.Object:
				default:

					if (t == typeof(DateTimeOffset))
					{
						return dateTimeOffsetWriter.WriteValue(context, (DateTimeOffset)value, buffer, offset);
					}
					if (t == typeof(Guid))
					{
						return guidWriter.WriteValue(context, (Guid)value, buffer, offset);
					}
					if (t == typeof(byte[]))
					{
						return binaryWriter.WriteValue(context, (byte[])value, buffer, offset);
					}
#if NET6_0_OR_GREATER
					if (t == typeof(DateOnly))
					{
						return dateOnlyWriter.WriteValue(context, (DateOnly)value, buffer, offset);
					}

					if (t == typeof(TimeOnly))
					{
						return timeOnlyWriter.WriteValue(context, (TimeOnly)value, buffer, offset);
					}
#endif
					break;
			}
			var str = value?.ToString() ?? string.Empty;
			return stringWriter.WriteValue(context, str, buffer, offset);
		}
	}

	sealed class StringFieldWriter : FieldWriter<string>
	{
		public static StringFieldWriter Instance = new();

		public override string GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetString(ordinal);
		}

		public override int WriteValue(WriterContext context, string value, char[] buffer, int offset)
		{
			var writer = context.writer;
			if (value.Length > 0 && value[0] == context.writer.comment)
				return writer.csvWriter.WriteEscaped(context, value, buffer, offset);

			return writer.csvWriter.Write(context, value, buffer, offset);
		}
	}

	sealed class BinaryBase64FieldWriter : FieldWriter<byte[]>
	{
		public static BinaryBase64FieldWriter Instance = new();

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

		public override byte[] GetValue(DbDataReader reader, int ordinal)
		{
			throw new InvalidOperationException();
		}

		public override int WriteValue(WriterContext context, byte[] value, char[] buffer, int offset)
		{
			throw new NotImplementedException();
		}
	}

	sealed class BinaryHexFieldWriter : FieldWriter<byte[]>
	{
		readonly static char[] HexMap = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		public static BinaryHexFieldWriter Instance = new();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var writer = context.writer;

			if (writer.dataBuffer.Length == 0)
			{
				// share a buffer with the base64 encoder.
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

		public override byte[] GetValue(DbDataReader reader, int ordinal)
		{
			throw new InvalidOperationException();
		}

		public override int WriteValue(WriterContext context, byte[] value, char[] buffer, int offset)
		{
			throw new NotImplementedException();
		}
	}

	sealed class BooleanFieldWriter : FieldWriter<bool>
	{
		public static BooleanFieldWriter Instance = new();

		public override bool GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetBoolean(ordinal);
		}

		public override int WriteValue(WriterContext context, bool value, char[] buffer, int offset)
		{
			var w = context.writer;
			var str = value ? w.trueString : w.falseString;
			return w.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class Int32FieldWriter : FieldWriter<int>
	{
		public static Int32FieldWriter Instance = new();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var value = reader.GetInt32(ordinal);
			return WriteValue(context, value, buffer, offset);
		}

		public override int GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetInt32(ordinal);
		}

		public override int WriteValue(WriterContext context, int value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
#if SPAN

			Span<char> str = stackalloc char[12];
			if (!value.TryFormat(str, out int len, default, culture))
			{
				throw new FormatException(); // this shouldn't happen
			}

			str = str[..len];

#else
			var str = value.ToString(culture);
#endif
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class Int64FieldWriter : FieldWriter<long>
	{
		public static Int64FieldWriter Instance = new();

		public override int Write(WriterContext context, int ordinal, char[] buffer, int offset)
		{
			var reader = context.reader;
			var value = reader.GetInt64(ordinal);
			return WriteValue(context, value, buffer, offset);
		}

		public override long GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetInt64(ordinal);
		}

		public override int WriteValue(WriterContext context, long value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
#if SPAN

			Span<char> str = stackalloc char[20];
			if (!value.TryFormat(str, out int len, default, culture))
			{
				throw new FormatException(); // this shouldn't happen
			}

			str = str[..len];

#else
			var str = value.ToString(culture);
#endif
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class SingleFieldWriter : FieldWriter<float>
	{
		public static SingleFieldWriter Instance = new();

		public override float GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFloat(ordinal);
		}
		
		public override int WriteValue(WriterContext context, float value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
#if SPAN

			Span<char> str = stackalloc char[14];
			if (!value.TryFormat(str, out int len, default, culture))
			{
				throw new FormatException(); // this shouldn't happen
			}

			str = str[..len];

#else
			var str = value.ToString(culture);
#endif
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class DoubleFieldWriter : FieldWriter<double>
	{
		public static DoubleFieldWriter Instance = new();

		public override double GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetDouble(ordinal);
		}

		public override int WriteValue(WriterContext context, double value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
#if SPAN

			Span<char> str = stackalloc char[16];
			if (!value.TryFormat(str, out int len, default, culture))
			{
				throw new FormatException(); // this shouldn't happen
			}

			str = str[..len];

#else
			var str = value.ToString(culture);
#endif
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class DecimalFieldWriter : FieldWriter<decimal>
	{
		public static DecimalFieldWriter Instance = new();

		public override decimal GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetDecimal(ordinal);
		}

		public override int WriteValue(WriterContext context, decimal value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
#if SPAN

			Span<char> str = stackalloc char[32];
			if (!value.TryFormat(str, out int len, default, culture))
			{
				throw new FormatException(); // this shouldn't happen
			}

			str = str[..len];

#else
			var str = value.ToString(culture);
#endif
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class GuidFieldWriter : FieldWriter<Guid>
	{
		public static GuidFieldWriter Instance = new();

		public override Guid GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetGuid(ordinal);
		}

		public override int WriteValue(WriterContext context, Guid value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
#if SPAN

			Span<char> str = stackalloc char[36];
			if (!value.TryFormat(str, out int len, default))
			{
				throw new FormatException(); // this shouldn't happen
			}

			str = str[..len];

#else
			var str = value.ToString();
#endif
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class DateTimeFormatFieldWriter : FieldWriter<DateTime>
	{
		public static DateTimeFormatFieldWriter Instance = new();

		public override DateTime GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetDateTime(ordinal);
		}

		public override int WriteValue(WriterContext context, DateTime value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
			// dateTimeFormat can be null here on ns2.0 (net4.8)
			var fmt = writer.dateTimeFormat ?? "O";
#if SPAN
			// this buffer might not be sufficiently large.
			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (value.TryFormat(str, out int len, fmt, culture))
			{
				// we were able to format to the stack buffer
				str = str[..len];
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
			else
			{
				// the stack buffer wasn't large enough, so allocate via ToString
				var roStr = value.ToString(fmt, culture).AsSpan();
				return writer.csvWriter.Write(context, roStr, buffer, offset);
			}
#else
			var str = value.ToString(fmt, culture);
			return writer.csvWriter.Write(context, str, buffer, offset);
#endif
		}
	}

	sealed class DateTimeOffsetFormatFieldWriter : FieldWriter<DateTimeOffset>
	{
		public static DateTimeOffsetFormatFieldWriter Instance = new();

		public override DateTimeOffset GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<DateTimeOffset>(ordinal);
		}

		public override int WriteValue(WriterContext context, DateTimeOffset value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;

			// dateTimeFormat can be null here on ns2.0 (net4.8)
			var fmt = writer.dateTimeOffsetFormat ?? "O";
#if SPAN
			// this buffer might not be sufficiently large.
			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (value.TryFormat(str, out int len, fmt, culture))
			{
				// we were able to format to the stack buffer
				str = str[..len];
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
			else
			{
				// the stack buffer wasn't large enough, so allocate via ToString
				var roStr = value.ToString(fmt, culture).AsSpan();
				return writer.csvWriter.Write(context, roStr, buffer, offset);
			}
#else
			var str = value.ToString(fmt, culture);
			return writer.csvWriter.Write(context, str, buffer, offset);
#endif
		}
	}

	sealed class TimeSpanFieldWriter : FieldWriter<TimeSpan>
	{
		public static TimeSpanFieldWriter Instance = new();

		public override TimeSpan GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<TimeSpan>(ordinal);
		}
		public override int WriteValue(WriterContext context, TimeSpan value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
			var fmt = writer.timeSpanFormat ?? "c";
#if SPAN
			// this buffer might not be sufficiently large.
			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (value.TryFormat(str, out int len, fmt, culture))
			{
				// we were able to format to the stack buffer
				str = str[..len];
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
			else
			{
				// the stack buffer wasn't large enough, so allocate via ToString
				var roStr = value.ToString(fmt, culture).AsSpan();
				return writer.csvWriter.Write(context, roStr, buffer, offset);
			}
#else
			var str = value.ToString(fmt, culture);
			return writer.csvWriter.Write(context, str, buffer, offset);
#endif
		}
	}


#if SPAN

sealed class DateTimeIsoFieldWriter : FieldWriter<DateTime>
	{
		public static DateTimeIsoFieldWriter Instance = new();

		public override DateTime GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetDateTime(ordinal);
		}

		public override int WriteValue(WriterContext context, DateTime value, char[] buffer, int offset)
		{
			var writer = context.writer;

			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (!IsoDate.TryFormatIso(value, str, out int len))
			{
				// this should never happen.
				throw new FormatException();
			}
			str = str.Slice(0, len);
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class DateTimeOffsetIsoFieldWriter : FieldWriter<DateTimeOffset>
	{
		public static DateTimeOffsetIsoFieldWriter Instance = new();

		public override DateTimeOffset GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<DateTimeOffset>(ordinal);
		}

		public override int WriteValue(WriterContext context, DateTimeOffset value, char[] buffer, int offset)
		{
			var writer = context.writer;

			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (!IsoDate.TryFormatIso(value, str, out int len))
			{
				// this should never happen.
				throw new FormatException();
			}
			str = str.Slice(0, len);
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class DateTimeIsoFastFieldWriter : FieldWriter<DateTime>
	{
		public static DateTimeIsoFastFieldWriter Instance = new();

		public override DateTime GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetDateTime(ordinal);
		}

		public override int WriteValue(WriterContext context, DateTime value, char[] buffer, int offset)
		{
			var span = new Span<char>(buffer, offset, buffer.Length - offset);
			return
				IsoDate.TryFormatIso(value, span, out int len)
				? len
				: InsufficientSpace;
		}
	}

	sealed class DateTimeOffsetIsoFastFieldWriter : FieldWriter<DateTimeOffset>
	{
		public static DateTimeOffsetIsoFastFieldWriter Instance = new();

		public override DateTimeOffset GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<DateTimeOffset>(ordinal);
		}

		public override int WriteValue(WriterContext context, DateTimeOffset value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			return
				IsoDate.TryFormatIso(value, span, out int len)
				? len
				: InsufficientSpace;
		}
	}

	sealed class Int32FastFieldWriter : FieldWriter<int>
	{
		public static Int32FastFieldWriter Instance = new();

		public override int GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetInt32(ordinal);
		}

		public override int WriteValue(WriterContext context, int value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			if (!value.TryFormat(span, out int len, default, CultureInfo.InvariantCulture))
			{
				return InsufficientSpace;
			}
			return len;
		}
	}

	sealed class Int64FastFieldWriter : FieldWriter<long>
	{
		public static Int64FastFieldWriter Instance = new();

		public override long GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetInt64(ordinal);
		}

		public override int WriteValue(WriterContext context, long value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			if (!value.TryFormat(span, out int len, default, CultureInfo.InvariantCulture))
			{
				return InsufficientSpace;
			}
			return len;
		}
	}

	

	sealed class TimeSpanFastFieldWriter : FieldWriter<TimeSpan>
	{
		public static TimeSpanFastFieldWriter Instance = new();

		public override TimeSpan GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<TimeSpan>(ordinal);
		}

		public override int WriteValue(WriterContext context, TimeSpan value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			if (!value.TryFormat(span, out int len, default, CultureInfo.InvariantCulture))
			{
				return InsufficientSpace;
			}
			return len;
		}
	}

#if NET6_0_OR_GREATER

	sealed class DateOnlyIsoFastFieldWriter : FieldWriter<DateOnly>
	{
		public static DateOnlyIsoFastFieldWriter Instance = new();

		public override DateOnly GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<DateOnly>(ordinal);
		}

		public override int WriteValue(WriterContext context, DateOnly value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			return IsoDate.TryFormatIso(value, span, out int len)
				? len
				: InsufficientSpace;
		}
	}

	sealed class DateOnlyIsoFieldWriter : FieldWriter<DateOnly>
	{
		public static DateOnlyIsoFieldWriter Instance = new();

		public override DateOnly GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<DateOnly>(ordinal);
		}

		public override int WriteValue(WriterContext context, DateOnly value, char[] buffer, int offset)
		{
			var writer = context.writer;
			Span<char> str = stackalloc char[IsoDate.MaxDateOnlyLength];
			if (!IsoDate.TryFormatIso(value, str, out int len))
			{
				// this should never happen.
				throw new FormatException();
			}
			str = str[..len];
			return writer.csvWriter.Write(context, str, buffer, offset);
		}
	}

	sealed class DateOnlyFormatFieldWriter : FieldWriter<DateOnly>
	{
		public static DateOnlyFormatFieldWriter Instance = new();

		public override DateOnly GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<DateOnly>(ordinal);
		}

		public override int WriteValue(WriterContext context, DateOnly value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
			var fmt = writer.dateOnlyFormat;
			// this buffer might not be sufficiently large.
			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (value.TryFormat(str, out int len, fmt, culture))
			{
				// we were able to format to the stack buffer
				str = str[..len];
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
			else
			{
				// the stack buffer wasn't large enough, so allocate via ToString
				var roStr = value.ToString(fmt, culture).AsSpan();
				return writer.csvWriter.Write(context, roStr, buffer, offset);
			}
		}
	}

	sealed class TimeOnlyFastFieldWriter : FieldWriter<TimeOnly>
	{
		public static TimeOnlyFastFieldWriter Instance = new();

		public override TimeOnly GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<TimeOnly>(ordinal);
		}

		public override int WriteValue(WriterContext context, TimeOnly value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			if (!value.TryFormat(span, out int len, default, CultureInfo.InvariantCulture))
			{
				return InsufficientSpace;
			}
			return len;
		}
	}

	sealed class TimeOnlyFormatFieldWriter : FieldWriter<TimeOnly>
	{
		public static TimeOnlyFormatFieldWriter Instance = new();

		public override TimeOnly GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<TimeOnly>(ordinal);
		}

		public override int WriteValue(WriterContext context, TimeOnly value, char[] buffer, int offset)
		{
			var writer = context.writer;
			var culture = writer.culture;
			var fmt = writer.timeOnlyFormat;
			Span<char> str = stackalloc char[IsoDate.MaxDateLength];
			if (value.TryFormat(str, out int len, fmt, culture))
			{
				// we were able to format to the stack buffer
				str = str[..len];
				return writer.csvWriter.Write(context, str, buffer, offset);
			}
			else
			{
				// the stack buffer wasn't large enough, so allocate via ToString
				var roStr = value.ToString(fmt, culture).AsSpan();
				return writer.csvWriter.Write(context, roStr, buffer, offset);
			}
		}
	}

#endif

	sealed class EnumFastFieldWriter<T> : FieldWriter<T> where T : Enum
	{
		public static EnumFastFieldWriter<T> Instance = new();

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

		public override T GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFieldValue<T>(ordinal);
		}

		public override int WriteValue(WriterContext context, T value, char[] buffer, int offset)
		{
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

	sealed class SingleFastFieldWriter : FieldWriter<float>
	{
		public static SingleFastFieldWriter Instance = new();

		public override float GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetFloat(ordinal);
		}

		public override int WriteValue(WriterContext context, float value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			if (!value.TryFormat(span, out int len, default, CultureInfo.InvariantCulture))
			{
				return InsufficientSpace;
			}
			return len;
		}
	}

	sealed class DoubleFastFieldWriter : FieldWriter<double>
	{
		public static DoubleFastFieldWriter Instance = new();

		public override double GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetDouble(ordinal);
		}

		public override int WriteValue(WriterContext context, double value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			if (!value.TryFormat(span, out int len, default, CultureInfo.InvariantCulture))
			{
				return InsufficientSpace;
			}
			return len;
		}
	}

	sealed class DecimalFastFieldWriter : FieldWriter<decimal>
	{
		public static DecimalFastFieldWriter Instance = new();

		public override decimal GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetDecimal(ordinal);
		}
		public override int WriteValue(WriterContext context, decimal value, char[] buffer, int offset)
		{
			var span = buffer.AsSpan(offset);
			if (!value.TryFormat(span, out int len, default, CultureInfo.InvariantCulture))
			{
				return InsufficientSpace;
			}
			return len;
		}
	}

	sealed class GuidFastFieldWriter : FieldWriter<Guid>
	{
		public static GuidFastFieldWriter Instance = new();

		public override Guid GetValue(DbDataReader reader, int ordinal)
		{
			return reader.GetGuid(ordinal);
		}

		public override int WriteValue(WriterContext context, Guid value, char[] buffer, int offset)
		{
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
