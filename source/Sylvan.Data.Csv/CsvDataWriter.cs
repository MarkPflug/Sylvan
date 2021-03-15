using System;
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
	public partial class CsvDataWriter
		: IDisposable
#if NETSTANDARD2_1
		, IAsyncDisposable
#endif
	{
		class FieldInfo
		{
			public FieldInfo(bool allowNull, Type type)
			{
				this.allowNull = allowNull;
				this.type = type;
				this.typeCode = Type.GetTypeCode(type);
			}
			public bool allowNull;
			public Type type;
			public TypeCode typeCode;
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
		readonly bool writeHeaders;
		readonly char delimiter;
		readonly char quote;
		readonly char escape;
		readonly char comment;
		readonly string newLine;

		readonly string trueString;
		readonly string falseString;
		readonly string? dateTimeFormat;

		readonly CultureInfo culture;

		byte[] dataBuffer = Array.Empty<byte>();
		readonly char[] buffer;
		int pos;

		bool disposedValue;

		readonly bool fastDouble;
		readonly bool fastInt;
		readonly bool fastDate;

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
			this.trueString = options.TrueString;
			this.falseString = options.FalseString;
			this.dateTimeFormat = options.DateTimeFormat;
			this.writeHeaders = options.WriteHeaders;
			this.delimiter = options.Delimiter;
			this.quote = options.Quote;
			this.escape = options.Escape;
			this.comment = options.Comment;
			this.newLine = options.NewLine;
			this.culture = options.Culture;
			this.buffer = options.Buffer ?? new char[options.BufferSize];
			this.pos = 0;

			var isInvariantCulture = this.culture == CultureInfo.InvariantCulture;
			this.fastDouble = isInvariantCulture && delimiter != '.' && quote != '.';
			this.fastInt = isInvariantCulture && delimiter != '-' && quote != '-';
			this.fastDate = isInvariantCulture && delimiter == ',' && quote == '\"';
		}

		// this can return either Complete or InsufficientSpace
		WriteResult WriteField(DbDataReader reader, FieldInfo[] fieldTypes, int i)
		{
			var allowNull = fieldTypes[i].allowNull;

			if (allowNull && reader.IsDBNull(i))
			{
				return WriteResult.Complete;
			}

			int intVal;
			string? str;
			WriteResult result = WriteResult.Complete;

			var typeCode = fieldTypes[i].typeCode;
			switch (typeCode)
			{
				case TypeCode.Boolean:
					var boolVal = reader.GetBoolean(i);
					result = WriteField(boolVal);
					break;
				case TypeCode.String:
					str = reader.GetString(i);
					if(i == 0 && str.Length > 0 && str[0] == comment)
					{
						result = WriteQuoted(str);
					} else {
						result = WriteField(str);
					}					
					break;
				case TypeCode.Byte:
					intVal = reader.GetByte(i);
					goto intVal;
				case TypeCode.Int16:
					intVal = reader.GetInt16(i);
					goto intVal;
				case TypeCode.Int32:
					intVal = reader.GetInt32(i);
				intVal:
					result = WriteField(intVal);
					break;
				case TypeCode.Int64:
					var longVal = reader.GetInt64(i);
					WriteField(longVal);
					break;
				case TypeCode.DateTime:
					var dateVal = reader.GetDateTime(i);
					WriteField(dateVal);
					break;
				case TypeCode.Single:
					var floatVal = reader.GetFloat(i);
					WriteField(floatVal);
					break;
				case TypeCode.Double:
					var doubleVal = reader.GetDouble(i);
					WriteField(doubleVal);
					break;
				case TypeCode.Empty:
				case TypeCode.DBNull:
					// nothing to do.
					result = WriteResult.Complete;
					break;
				default:
					var type = fieldTypes[i].type;
					if (type == typeof(byte[]))
					{
						if (dataBuffer.Length == 0)
						{
							dataBuffer = new byte[Base64EncSize];
						}
						var idx = 0;
						if (IsBase64Symbol(delimiter) || IsBase64Symbol(quote))
						{
							throw new InvalidOperationException();
						}
						int len;
						var pos = this.pos;
						while ((len = (int)reader.GetBytes(i, idx, dataBuffer, 0, Base64EncSize)) != 0)
						{
							var req = (len + 2) / 3 * 4;
							if (pos + req >= buffer.Length)
								return WriteResult.InsufficientSpace;

							var c = Convert.ToBase64CharArray(dataBuffer, 0, len, this.buffer, pos);
							
							idx += len;
							pos += c;
						}
						this.pos = pos;
						result = WriteResult.Complete;
						break;
					}
					if (type == typeof(Guid))
					{
						var guid = reader.GetGuid(i);
						result = WriteField(guid);
						break;
					}
					str = reader.GetValue(i)?.ToString() ?? string.Empty;
					result = WriteField(str);
					break;
			}
			return result;
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
#if NETSTANDARD2_1
			if(fastDouble)
			{
				if(value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
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
#if NETSTANDARD2_1
			if(fastInt)
			{
				if(value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
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
#if NETSTANDARD2_1
			if(fastInt)
			{
				if(value.TryFormat(buffer.AsSpan()[pos..], out int len, default, culture))
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
#if NETSTANDARD2_1
			if (fastDate)
			{
				if (value.TryFormat(buffer.AsSpan()[pos..], out int len, dateTimeFormat, culture))
				{
					pos += len;
					return WriteResult.Complete;
				}
				return WriteResult.InsufficientSpace;
			}
#endif
			return WriteField(value.ToString(dateTimeFormat, culture));
		}

		WriteResult WriteField(string value)
		{
			var r = WriteValueOptimistic(value);
			if (r == WriteResult.RequiresEscaping)
			{
				return WriteQuoted(value);
			}
			return r;
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
				if (c == delimiter || c == quote || c == '\n' || c == '\r')
				{
					return WriteResult.RequiresEscaping;
				}
				buffer[pos + i] = c;
			}
			this.pos += str.Length;
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

				if (c == delimiter || c == quote || c == '\r' || c == '\n')
				{
					if (c == quote || c == escape)
					{
						if (p == buffer.Length)
							return WriteResult.InsufficientSpace;
						buffer[p++] = escape;
					}
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
	}
}
