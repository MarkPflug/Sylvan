using System;
using System.Data.Common;
using System.Globalization;
using System.IO;
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

		readonly TextWriter writer;
		readonly bool writeHeaders;
		readonly char delimiter;
		readonly char quote;
		readonly char escape;
		readonly string newLine;

		readonly string trueString;
		readonly string falseString;
		readonly string? dateTimeFormat;

		readonly CultureInfo culture;

		char[] prepareBuffer;
		byte[]? dataBuffer = null;
		readonly char[] buffer;
		readonly int bufferSize;
		int pos;
		
		bool[] needsEscape;

		readonly bool invariantCulture;
	
		bool disposedValue;

		static TextWriter GetWriter(string fileName, CsvDataWriterOptions? options)
		{
			var bufferSize = options?.BufferSize ?? options?.Buffer?.Length ?? CsvDataWriterOptions.Default.BufferSize;
			var fs = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read, bufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);
			return new StreamWriter(fs, Encoding.UTF8, bufferSize);
		}

		/// <summary>
		/// Creates a new CsvDataWriter.
		/// </summary>
		/// <param name="fileName">The path of the file to write.</param>
		/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
		public static CsvDataWriter Create(string fileName, CsvDataWriterOptions? options = null)
		{
			var writer = GetWriter(fileName, options);
			return new CsvDataWriter(writer, options);
		}

		/// <summary>
		/// Creates a new CsvDataWriter.
		/// </summary>
		/// <param name="writer">The TextWriter to receive the delimited data.</param>
		/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
		public static CsvDataWriter Create(TextWriter writer, CsvDataWriterOptions? options = null)
		{
			return new CsvDataWriter(writer, options);
		}

		CsvDataWriter(TextWriter writer, CsvDataWriterOptions? options = null)
		{
			if (writer == null) throw new ArgumentNullException(nameof(writer));
			if (options != null)
			{
				options.Validate();
			}
			else
			{
				options = CsvDataWriterOptions.Default;
			}

			if (writer == null) throw new ArgumentNullException(nameof(writer));
			if (options != null)
			{
				options.Validate();
			}
			else
			{
				options = CsvDataWriterOptions.Default;
			}

			this.writer = writer;
			this.trueString = options.TrueString;
			this.falseString = options.FalseString;
			this.dateTimeFormat = options.DateTimeFormat;
			this.writeHeaders = options.WriteHeaders;
			this.delimiter = options.Delimiter;
			this.quote = options.Quote;
			this.escape = options.Escape;
			this.newLine = options.NewLine;
			this.culture = options.Culture;
			this.invariantCulture = this.culture == CultureInfo.InvariantCulture;
			this.prepareBuffer = new char[0x100];
			this.bufferSize = options.BufferSize;
			this.buffer = options.Buffer ?? new char[bufferSize];			
			this.pos = 0;
			this.needsEscape = new bool[128];
			this.needsEscape[quote] = true;
			this.needsEscape[delimiter] = true;
			this.needsEscape['\r'] = true;
			this.needsEscape['\n'] = true;
		}

		const int Base64EncSize = 3 * 256; // must be a multiple of 3.

		void PrepareValue(string str, out int o, out int l)
		{
			var worstCaseLenth = str.Length * 2 + 2;
			// at worst, we'll have to escape every character and put quotes around it

			if (this.prepareBuffer.Length < worstCaseLenth)
			{
				if (str.Length > bufferSize)
				{
					// the value being written is too large to fit in the buffer
					throw new ArgumentOutOfRangeException();
				}
				Array.Resize(ref this.prepareBuffer, worstCaseLenth);
			}

			var buffer = this.prepareBuffer;
			var p = 0;
			buffer[p++] = quote;
			bool isQuoted = false;
			for (int i = 0; i < str.Length; i++)
			{
				var c = str[i];

				if (c == delimiter || c == quote || c == '\r' || c == '\n')
				{
					isQuoted = true;
					if (c == quote || c == escape)
					{
						buffer[p++] = escape;
					}
				}
				buffer[p++] = c;
			}
			buffer[p++] = quote;

			if (isQuoted)
			{
				o = 0;
				l = p;
			}
			else
			{
				o = 1;
				l = p - 2;
			}
		}

		enum WriteResult
		{
			InsufficientSpace = 1,
			RequiresEscaping,
			Complete,
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
					result = WriteField(str);
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
						if (dataBuffer == null)
						{
							dataBuffer = new byte[Base64EncSize];
						}
						var idx = 0;
						AssertBinaryPrereq(0);
						int len = 0;
						while ((len = (int)reader.GetBytes(i, idx, dataBuffer, 0, Base64EncSize)) != 0)
						{
							WriteBinaryValue(dataBuffer, len);
							idx += len;
						}
						break;
					}
					if (type == typeof(Guid))
					{
						var guid = reader.GetGuid(i);
						WriteField(guid);
						break;
					}
					str = reader.GetValue(i)?.ToString() ?? string.Empty;
					result = WriteField(str);
					break;
			}
			return result;
		}

		WriteResult WriteValue(string str)
		{
			int o, l;
			PrepareValue(str, out o, out l);
			return WriteValue(this.prepareBuffer, o, l);
		}

		WriteResult WriteValue(char[] buffer, int o, int l)
		{
			if (pos + l >= bufferSize) return WriteResult.InsufficientSpace;

			Array.Copy(buffer, o, this.buffer, pos, l);
			pos += l;
			return WriteResult.Complete;
		}

		WriteResult WriteField(Guid value)
		{
			if (!IsAvailable(64)) return WriteResult.InsufficientSpace;

			// keep it fast/allocation-free in the sane case.
			if (delimiter == '-' || quote == '-')
			{
				WriteValue(value.ToString());
			}
			else
			{
				WriteValueInvariant(value);
			}
			return WriteResult.Complete;
		}

		WriteResult WriteValueOptimistic(string str)
		{
			var start = pos;
			var ne = this.needsEscape;
			for (int i = 0; i < str.Length; i++)
			{
				var c = str[i];
				if (c < 128 && ne[c])
				{
					pos = start;
					return WriteResult.RequiresEscaping;
				}
				if (pos == bufferSize)
				{
					pos = start;
					return WriteResult.InsufficientSpace;
				}
				buffer[pos++] = c;
			}
			return WriteResult.Complete;
		}

		WriteResult WriteValueInvariant(int value)
		{
#if NETSTANDARD2_1
			var span = buffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Complete;
			}
			return WriteResult.InsufficientSpace;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(int value)
		{
			if (invariantCulture)
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.RequiresEscaping;
		}

		WriteResult WriteValue(int value)
		{
			if (invariantCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString();
			return WriteValue(str);
		}

		WriteResult WriteValueInvariant(long value)
		{
#if NETSTANDARD2_1
			var span = buffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Complete;
			}
			return WriteResult.InsufficientSpace;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(long value)
		{
			if (invariantCulture)
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.RequiresEscaping;
		}

		WriteResult WriteValue(long value)
		{
			if (invariantCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString(culture);
			return WriteValue(str);
		}

		WriteResult WriteValueInvariant(DateTime value)
		{
#if NETSTANDARD2_1
			var span = buffer.AsSpan()[pos..bufferSize];

			if (value.TryFormat(span, out int c, this.dateTimeFormat.AsSpan(), culture))
			{
				pos += c;
				return WriteResult.Complete;
			}
			return WriteResult.InsufficientSpace;
#else
			var str =
				dateTimeFormat == null
				? value.ToString(culture)
				: value.ToString(dateTimeFormat, culture);

			return WriteValue(str);
#endif
		}

		WriteResult WriteValueInvariant(Guid value)
		{
#if NETSTANDARD2_1
			var span = buffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c))
			{
				pos += c;
				return WriteResult.Complete;
			}
			return WriteResult.InsufficientSpace;
#else
			var str = value.ToString();
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(DateTime value)
		{
			if (invariantCulture)
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.RequiresEscaping;
		}

		WriteResult WriteValue(DateTime value)
		{
			if (invariantCulture)
			{
				return WriteValueInvariant(value);
			}
			var str =
				this.dateTimeFormat == null
				? value.ToString(culture)
				: value.ToString(dateTimeFormat, culture);

			return WriteValue(str);
		}

		WriteResult WriteValueInvariant(double value)
		{
#if NETSTANDARD2_1
			var span = buffer.AsSpan()[pos..bufferSize];
			if (value.TryFormat(span, out int c, provider: culture))
			{
				pos += c;
				return WriteResult.Complete;
			}
			return WriteResult.InsufficientSpace;
#else
			var str = value.ToString(culture);
			return WriteValue(str);
#endif
		}

		WriteResult WriteValueOptimistic(double value)
		{
			if (invariantCulture && delimiter != '.')
			{
				return WriteValueInvariant(value);
			}
			return WriteResult.RequiresEscaping;
		}

		WriteResult WriteValue(double value)
		{
			if (invariantCulture)
			{
				return WriteValueInvariant(value);
			}
			var str = value.ToString(culture);
			return WriteValue(str);
		}

		// this should only be called in scenarios where we know there is enough room.
		void EndRecord()
		{
			var nl = this.newLine;
			for (int i = 0; i < nl.Length; i++)
				buffer[pos++] = nl[i];
		}

		bool IsAvailable(int size)
		{
			return pos + size < bufferSize;
		}

		void AssertBinaryPrereq(int size)
		{
			// punt these crazy scenarios.
			if (IsBase64Symbol(delimiter) || IsBase64Symbol(quote))
			{
				throw new InvalidOperationException();
			}
			if (size > buffer.Length)
				throw new InvalidOperationException();
		}

		static bool IsBase64Symbol(char c)
		{
			return c == '+' || c == '/' || c == '=';
		}

		WriteResult WriteField(bool value)
		{
			var str = value ? trueString : falseString;
			if (!IsAvailable(str.Length))
				return WriteResult.InsufficientSpace;
			var r = WriteValueOptimistic(str);
			if (r == WriteResult.RequiresEscaping)
			{
				return WriteValue(str);
			}
			return r;
		}

		WriteResult WriteField(double value)
		{
			if (!IsAvailable(64))
				return WriteResult.InsufficientSpace;
			var r = WriteValueOptimistic(value);
			if (r == WriteResult.RequiresEscaping)
			{

				return WriteValue(value);
			}
			return r;
		}

		WriteResult WriteField(int value)
		{
			var r = WriteValueOptimistic(value);
			if (r == WriteResult.RequiresEscaping)
			{
				WriteValue(value);
			}
			return WriteResult.Complete;
		}

		WriteResult WriteField(DateTime value)
		{
			if (!IsAvailable(64))
			{
				return WriteResult.InsufficientSpace;
			}
			var r = WriteValueOptimistic(value);
			if (r == WriteResult.RequiresEscaping)
			{
				return WriteValue(value);
			}
			return r;
		}

		WriteResult WriteField(string value)
		{
			var r = WriteValueOptimistic(value);
			if (r == WriteResult.RequiresEscaping)
			{
				return WriteValue(value);
			}
			return r;
		}


		int WriteBinaryValue(byte[] buffer, int len)
		{
			var written = Convert.ToBase64CharArray(buffer, 0, len, this.buffer, pos);
			pos += written;
			return written;
		}
	}
}
