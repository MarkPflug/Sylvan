#if NET6_0_OR_GREATER

using System;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Sylvan.Data;

static partial class DataExtensions
{
	/// <summary>
	/// Writes a data set to a UTF-8 encoded JSON stream.
	/// </summary>
	/// <returns>The number of records written.</returns>
	public static long WriteJson(this DbDataReader data, Stream stream)
	{
		using var writer = new Utf8JsonWriter(stream);
		return WriteJson(data, writer);
	}

	/// <summary>
	/// Asynchronously writes a data set to a UTF-8 encoded JSON stream.
	/// </summary>
	/// <returns>The number of records written.</returns>
	public static Task<long> WriteJsonAsync(this DbDataReader data, Stream stream)
	{
		using var writer = new Utf8JsonWriter(stream);
		return WriteJsonAsync(data, writer);
	}

	/// <summary>
	/// Writes a data set to a Utf8JsonWriter.
	/// </summary>
	/// <returns>The number of records written.</returns>
	public static long WriteJson(this DbDataReader data, Utf8JsonWriter writer)
	{
		writer.WriteStartArray();
		long count = 0;

		var names = GetNames(data);

		while (data.Read())
		{
			count++;
			WriteRecord(data, names, writer);
		}

		writer.WriteEndArray();
		return count;
	}
	/// <summary>
	/// Asynchronously writes a data set to a Utf8JsonWriter.
	/// </summary>
	/// <returns>The number of records written.</returns>
	public static async Task<long> WriteJsonAsync(this DbDataReader data, Utf8JsonWriter writer)
	{
		writer.WriteStartArray();
		long count = 0;

		var names = GetNames(data);

		while (await data.ReadAsync().ConfigureAwait(false))
		{
			count++;
			WriteRecord(data, names, writer);
		}

		writer.WriteEndArray();
		return count;
	}

	static JsonEncodedText[] GetNames(DbDataReader data)
	{
		var fieldCount = data.FieldCount;
		var names = new JsonEncodedText[fieldCount];
		for (int i = 0; i < fieldCount; i++)
		{
			names[i] = JsonEncodedText.Encode(data.GetName(i));
		}
		return names;
	}

	const string DateTimeUtcFormat = "yyyy-MM-ddTHH:mm:ss.FFFFFFFZ";
	const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.FFFFFFF";
	const string DateTimeOffsetFormat = "yyyy-MM-ddTHH:mm:ss.FFFFFFFK";

	static void WriteRecord(DbDataReader data, JsonEncodedText[] names, Utf8JsonWriter writer)
	{
		writer.WriteStartObject();

		// scratch buffer for formatting, large enough for DateTime/Offset/Guid
		Span<char> buffer = stackalloc char[36];
		int len;

		for (int i = 0; i < names.Length; i++)
		{
			writer.WritePropertyName(names[i]);
			if (data.IsDBNull(i))
			{
				writer.WriteNullValue();
			}
			else
			{
				var t = data.GetFieldType(i);
				var c = Type.GetTypeCode(t);
				switch (c)
				{
					case TypeCode.Boolean:
						writer.WriteBooleanValue(data.GetBoolean(i));
						break;
					case TypeCode.Byte:
						writer.WriteNumberValue(data.GetByte(i));
						break;
					case TypeCode.Int16:
						writer.WriteNumberValue(data.GetInt16(i));
						break;
					case TypeCode.Int32:
						writer.WriteNumberValue(data.GetInt32(i));
						break;
					case TypeCode.Int64:
						writer.WriteNumberValue(data.GetInt64(i));
						break;
					case TypeCode.Single:
						writer.WriteNumberValue(data.GetFloat(i));
						break;
					case TypeCode.Double:
						writer.WriteNumberValue(data.GetDouble(i));
						break;
					case TypeCode.Decimal:
						writer.WriteNumberValue(data.GetDecimal(i));
						break;
					case TypeCode.String:
						writer.WriteStringValue(data.GetString(i));
						break;
					case TypeCode.DateTime:
						var dt = data.GetDateTime(i);
						string fmt = DateTimeUtcFormat;
						switch (dt.Kind)
						{
							case DateTimeKind.Unspecified:
								fmt = DateTimeFormat;
								break;
							case DateTimeKind.Local:
								dt = dt.ToUniversalTime();
								break;
							case DateTimeKind.Utc:
								break;
						}

						if (!dt.TryFormat(buffer, out len, fmt, CultureInfo.InvariantCulture))
						{
							throw new FormatException();
						}

						writer.WriteStringValue(buffer.Slice(0, len));
						break;
					default:
						if (t == typeof(DateTimeOffset))
						{
							var dto = data.GetFieldValue<DateTimeOffset>(i);
							if (!dto.TryFormat(buffer, out len, DateTimeOffsetFormat, CultureInfo.InvariantCulture))
							{
								throw new FormatException();
							}

							writer.WriteStringValue(buffer.Slice(0, len));
							break;
						}

						if (t == typeof(Guid))
						{
							var guidVal = data.GetGuid(i);
							if (!guidVal.TryFormat(buffer, out len))
							{
								throw new FormatException();
							}

							writer.WriteStringValue(buffer.Slice(0, len));
							break;
						}

						if (t == typeof(byte[]))
						{
							byte[] value = (byte[])data.GetValue(i);
							writer.WriteBase64StringValue(value);
							break;
						}

						if (t == typeof(char[]))
						{
							char[] value = (char[])data.GetValue(i);
							writer.WriteStringValue(value);
							break;
						}

						// handle everything else
						var obj = data.GetValue(i);
						if (obj == DBNull.Value || obj == null)
						{
							writer.WriteNullValue();
						}
						else
						{
							writer.WriteStringValue(obj.ToString());
						}
						break;
				}
			}
		}

		writer.WriteEndObject();
	}
}

#endif