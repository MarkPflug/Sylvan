using System;
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
				return StringFieldWriter.Instance;
			if (type == typeof(int))
			{
				return Int32FastFieldWriter.Instance;
			}
			if (type == typeof(double))
			{
				return DoubleFastFieldWriter.Instance;
				//return DoubleFieldWriter.Instance;
			}
			if (type == typeof(bool))
				return BooleanFieldWriter.Instance;
			if (type == typeof(DateTime))
			{
				return DateTimeFastFieldWriter.Instance;
			}

			throw new NotSupportedException("Type of " + type.FullName + " not supported");
			//return ObjectFieldWriter.Instance;
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
			this.csvWriter = options.Style ==  CsvStyle.Standard ? CsvWriter.Quoted : CsvWriter.Escaped;
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

//		WriteResult WriteField(Guid value)
//		{
//			return WriteField(value.ToString());
//		}

//		WriteResult WriteField(bool value)
//		{
//			var str = value ? trueString : falseString;
//			return WriteField(str);
//		}

//		WriteResult WriteField(double value)
//		{
//#if SPAN
//			if (fastDouble)
//			{
//				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
//				{
//					pos += len;
//					return WriteResult.Complete;
//				}
//				return WriteResult.InsufficientSpace;
//			}
//#endif
//			return WriteField(value.ToString(culture));
//		}

//		WriteResult WriteField(long value)
//		{
//#if SPAN
//			if (fastInt)
//			{
//				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
//				{
//					pos += len;
//					return WriteResult.Complete;
//				}
//				return WriteResult.InsufficientSpace;
//			}
//#endif
//			return WriteField(value.ToString(culture));
//		}

//		WriteResult WriteField(int value)
//		{
//#if SPAN
//			if (fastInt)
//			{
//				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
//				{
//					pos += len;
//					return WriteResult.Complete;
//				}
//				return WriteResult.InsufficientSpace;
//			}
//#endif
//			return WriteField(value.ToString(culture));
//		}

//		WriteResult WriteField(DateTime value)
//		{
//			var format = value.TimeOfDay == TimeSpan.Zero ? dateFormat : dateTimeFormat;
//#if SPAN
//			if (fastDate)
//			{
//				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, format, culture))
//				{
//					pos += len;
//					return WriteResult.Complete;
//				}
//				return WriteResult.InsufficientSpace;
//			}
//#endif
//			return WriteField(value.ToString(format, culture));
//		}

//		WriteResult WriteField(TimeSpan value)
//		{
//#if SPAN
//			if (fastDate)
//			{
//				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
//				{
//					pos += len;
//					return WriteResult.Complete;
//				}
//				return WriteResult.InsufficientSpace;
//			}
//#endif
//			return WriteField(value.ToString(null, culture));
//		}

//#if NET6_0_OR_GREATER

//		WriteResult WriteField(DateOnly value)
//		{
//			var format = dateFormat;

//			if (fastDate)
//			{
//				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, format, culture))
//				{
//					pos += len;
//					return WriteResult.Complete;
//				}
//				return WriteResult.InsufficientSpace;
//			}
//			return WriteField(value.ToString(format, culture));
//		}

//		WriteResult WriteField(TimeOnly value)
//		{
//			if (fastDate)
//			{
//				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
//				{
//					pos += len;
//					return WriteResult.Complete;
//				}
//				return WriteResult.InsufficientSpace;
//			}
//			return WriteField(value.ToString(null, culture));
//		}
//#endif

		//WriteResult WriteField(string value)
		//{
		//	if (style == CsvStyle.Standard)
		//	{
		//		var r = WriteValueOptimistic(value);
		//		if (r == WriteResult.RequiresEscaping)
		//		{
		//			return WriteQuoted(value);
		//		}
		//		return r;
		//	}
		//	else
		//	{
		//		return WriteEscapedValue(value);
		//	}
		//}

		
	}
}
