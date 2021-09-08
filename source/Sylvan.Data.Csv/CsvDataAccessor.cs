using System;
using System.Collections.Generic;
using System.IO;

namespace Sylvan.Data.Csv
{
	// these accessors support the GetFieldValue<T> generic accessor method.
	// most of them defer to CsvDataReader.GetXXX methods.

	interface IFieldAccessor<T>
	{
		T GetValue(CsvDataReader reader, int ordinal);
	}

	interface IFieldAccessor
	{
		object GetValueAsObject(CsvDataReader reader, int ordinal);
	}

	interface IFieldRangeAccessor<T>
	{
		long GetRange(CsvDataReader reader, long dataOffset, int ordinal, T[] buffer, int bufferOffset, int length);
	}

	abstract class FieldAccessor<T> : IFieldAccessor<T>, IFieldAccessor
	{
		public object GetValueAsObject(CsvDataReader reader, int ordinal)
		{
			return (object)this.GetValue(reader, ordinal)!;
		}

		public abstract T GetValue(CsvDataReader reader, int ordinal);
	}

	sealed class StringAccessor : FieldAccessor<string>
	{
		internal static readonly StringAccessor Instance = new StringAccessor();

		public override string GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetString(ordinal);
		}
	}

	sealed class BooleanAccessor : FieldAccessor<bool>
	{

		internal static readonly BooleanAccessor Instance = new BooleanAccessor();

		public override bool GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetBoolean(ordinal);
		}
	}

	sealed class CharAccessor : FieldAccessor<char>
	{
		internal static readonly CharAccessor Instance = new CharAccessor();

		public override char GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetChar(ordinal);
		}
	}

	sealed class ByteAccessor : FieldAccessor<byte>
	{
		internal static readonly ByteAccessor Instance = new ByteAccessor();

		public override byte GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetByte(ordinal);
		}
	}

	sealed class Int16Accessor : FieldAccessor<short>
	{
		internal static readonly Int16Accessor Instance = new Int16Accessor();

		public override short GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetInt16(ordinal);
		}
	}

	sealed class Int32Accessor : FieldAccessor<int>
	{
		internal static readonly Int32Accessor Instance = new Int32Accessor();

		public override int GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetInt32(ordinal);
		}
	}

	sealed class Int64Accessor : FieldAccessor<long>
	{
		internal static readonly Int64Accessor Instance = new Int64Accessor();

		public override long GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetInt64(ordinal);
		}
	}

	sealed class SingleAccessor : FieldAccessor<float>
	{
		internal static readonly SingleAccessor Instance = new SingleAccessor();

		public override float GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetFloat(ordinal);
		}
	}

	sealed class DoubleAccessor : FieldAccessor<double>
	{
		internal static readonly DoubleAccessor Instance = new DoubleAccessor();

		public override double GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDouble(ordinal);
		}
	}

	sealed class DecimalAccessor : FieldAccessor<decimal>
	{
		internal static readonly DecimalAccessor Instance = new DecimalAccessor();

		public override decimal GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDecimal(ordinal);
		}
	}

	sealed class DateTimeAccessor : FieldAccessor<DateTime>
	{
		internal static readonly DateTimeAccessor Instance = new DateTimeAccessor();

		public override DateTime GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDateTime(ordinal);
		}
	}

	sealed class DateTimeOffsetAccessor : FieldAccessor<DateTimeOffset>
	{
		internal static readonly DateTimeOffsetAccessor Instance = new DateTimeOffsetAccessor();

		public override DateTimeOffset GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDateTimeOffset(ordinal);
		}
	}

#if NET6_0_OR_GREATER
	sealed class DateAccessor : FieldAccessor<DateOnly>
	{
		public override DateOnly GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDate(ordinal);
		}
	}

	sealed class TimeAccessor : FieldAccessor<TimeOnly>
	{
		public override TimeOnly GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetTime(ordinal);
		}
	}

#endif

	sealed class TimeSpanAccessor : FieldAccessor<TimeSpan>
	{
		internal static readonly TimeSpanAccessor Instance = new TimeSpanAccessor();

		public override TimeSpan GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetTimeSpan(ordinal);
		}
	}

	sealed class GuidAccessor : FieldAccessor<Guid>
	{
		internal static readonly GuidAccessor Instance = new GuidAccessor();

		public override Guid GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetGuid(ordinal);
		}
	}

	sealed class StreamAccessor : FieldAccessor<Stream>
	{
		internal static readonly StreamAccessor Instance = new StreamAccessor();

		public override Stream GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetStream(ordinal);
		}
	}

	sealed class TextReaderAccessor : FieldAccessor<TextReader>
	{
		internal static readonly TextReaderAccessor Instance = new TextReaderAccessor();

		public override TextReader GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetTextReader(ordinal);
		}
	}

	sealed class BytesAccessor : FieldAccessor<byte[]>
	{
		internal static readonly BytesAccessor Instance = new BytesAccessor();

		public override byte[] GetValue(CsvDataReader reader, int ordinal)
		{
			var len = reader.GetBytes(ordinal, 0, null, 0, 0);
			var buffer = new byte[len];
			reader.GetBytes(ordinal, 0, buffer, 0, buffer.Length);
			return buffer;
		}
	}

	sealed class CharsAccessor : FieldAccessor<char[]>
	{
		internal static readonly CharsAccessor Instance = new CharsAccessor();

		public override char[] GetValue(CsvDataReader reader, int ordinal)
		{
			var len = reader.GetChars(ordinal, 0, null, 0, 0);
			var buffer = new char[len];
			reader.GetChars(ordinal, 0, buffer, 0, buffer.Length);
			return buffer;
		}
	}

	sealed partial class CsvDataAccessor :
		IFieldAccessor<string>,
		IFieldAccessor<bool>,
		IFieldAccessor<char>,
		IFieldAccessor<byte>,
		IFieldAccessor<short>,
		IFieldAccessor<int>,
		IFieldAccessor<long>,
		IFieldAccessor<float>,
		IFieldAccessor<double>,
		IFieldAccessor<decimal>,
		IFieldAccessor<DateTime>,
		IFieldAccessor<DateTimeOffset>,
		IFieldAccessor<TimeSpan>,
		IFieldAccessor<Guid>,
		IFieldAccessor<Stream>,
		IFieldAccessor<TextReader>,
		IFieldAccessor<byte[]>,
		IFieldAccessor<char[]>,
		IFieldRangeAccessor<byte>,
		IFieldRangeAccessor<char>
	{
		internal static readonly CsvDataAccessor Instance = new CsvDataAccessor();

		internal static readonly Dictionary<Type, IFieldAccessor> Accessors;

		

		static CsvDataAccessor()
		{
			Accessors = new Dictionary<Type, IFieldAccessor>
			{
				{typeof(string), StringAccessor.Instance },
				{typeof(bool), BooleanAccessor.Instance },
				{typeof(char), CharAccessor.Instance },
				{typeof(byte), ByteAccessor.Instance },
				{typeof(short), Int16Accessor.Instance },
				{typeof(int), Int32Accessor.Instance },
				{typeof(long), Int64Accessor.Instance },
				{typeof(float), SingleAccessor.Instance },
				{typeof(double), DoubleAccessor.Instance },
				{typeof(decimal), DecimalAccessor.Instance },
				{typeof(DateTime), DateTimeAccessor.Instance },
				{typeof(DateTimeOffset), DateTimeOffsetAccessor.Instance },
				{typeof(TimeSpan), TimeSpanAccessor.Instance },
				{typeof(Guid), GuidAccessor.Instance },
				{typeof(Stream), StreamAccessor.Instance },
				{typeof(TextReader), TextReaderAccessor.Instance },
				{typeof(byte[]), BytesAccessor.Instance },
				{typeof(char[]), CharsAccessor.Instance },

			};
		}

		internal static IFieldAccessor? GetAccessor(Type type)
		{
			return Accessors.TryGetValue(type, out var acc) ? acc : null;
		}

		string IFieldAccessor<string>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetString(ordinal);
		}

		long IFieldRangeAccessor<byte>.GetRange(CsvDataReader reader, long dataOffset, int ordinal, byte[] buffer, int bufferOffset, int length)
		{
			return reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
		}

		long IFieldRangeAccessor<char>.GetRange(CsvDataReader reader, long dataOffset, int ordinal, char[] buffer, int bufferOffset, int length)
		{
			return reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
		}

		char IFieldAccessor<char>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetChar(ordinal);
		}

		bool IFieldAccessor<bool>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetBoolean(ordinal);
		}

		byte IFieldAccessor<byte>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetByte(ordinal);
		}

		short IFieldAccessor<short>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetInt16(ordinal);
		}

		int IFieldAccessor<int>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetInt32(ordinal);
		}

		long IFieldAccessor<long>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetInt64(ordinal);
		}

		Guid IFieldAccessor<Guid>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetGuid(ordinal);
		}

		float IFieldAccessor<float>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetFloat(ordinal);
		}

		double IFieldAccessor<double>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDouble(ordinal);
		}

		DateTime IFieldAccessor<DateTime>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDateTime(ordinal);
		}

		DateTimeOffset IFieldAccessor<DateTimeOffset>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDateTimeOffset(ordinal);
		}

		TimeSpan IFieldAccessor<TimeSpan>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetTimeSpan(ordinal);
		}

		decimal IFieldAccessor<decimal>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetDecimal(ordinal);
		}

		byte[] IFieldAccessor<byte[]>.GetValue(CsvDataReader reader, int ordinal)
		{
			var len = reader.GetBytes(ordinal, 0, null, 0, 0);
			var buffer = new byte[len];
			reader.GetBytes(ordinal, 0, buffer, 0, (int)len);
			return buffer;
		}

		char[] IFieldAccessor<char[]>.GetValue(CsvDataReader reader, int ordinal)
		{
			var len = reader.GetChars(ordinal, 0, null, 0, 0);
			var buffer = new char[len];
			reader.GetChars(ordinal, 0, buffer, 0, (int)len);
			return buffer;
		}

		Stream IFieldAccessor<Stream>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetStream(ordinal);
		}

		TextReader IFieldAccessor<TextReader>.GetValue(CsvDataReader reader, int ordinal)
		{
			return reader.GetTextReader(ordinal);
		}
	}
}
