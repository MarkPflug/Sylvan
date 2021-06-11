using System;
using System.IO;

namespace Sylvan.Data.Csv
{
	interface IFieldAccessor<T>
	{
		T GetValue(CsvDataReader reader, int ordinal);
	}

	interface IFieldRangeAccessor<T>
	{
		long GetRange(CsvDataReader reader, long dataOffset, int ordinal, T[] buffer, int bufferOffset, int length);
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
