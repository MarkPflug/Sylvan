#if NET6_0_OR_GREATER

using System;
using System.Data.Common;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Sylvan.Data;

static partial class DataExtensions {

	/// <summary>
	/// Write a data set to a UTF-8 encoded JSON stream;
	/// </summary>
	/// <returns>The number of records written.</returns>
	public static long WriteJson(this DbDataReader data, Stream stream)
	{
		using var writer = new Utf8JsonWriter(stream);

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
	/// Write a data set to a UTF-8 encoded JSON stream;
	/// </summary>
	/// <returns>The number of records written.</returns>
	public static async Task<long> WriteJsonAsync(this DbDataReader data, Stream stream)
	{
		using var writer = new Utf8JsonWriter(stream);

		writer.WriteStartArray();
		long count = 0;

		var names = GetNames(data);

		while (await data.ReadAsync())
		{
			count++;
			WriteRecord(data, names, writer);
			await writer.FlushAsync();
		}

		writer.WriteEndArray();
		await writer.FlushAsync();
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

	static void WriteRecord(DbDataReader data, JsonEncodedText[] names, Utf8JsonWriter writer)
	{
		writer.WriteStartObject();

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
						string fmt = "yyyy-MM-ddTHH:mm:ss.fffZ";
						switch (dt.Kind)
						{
							case DateTimeKind.Unspecified:
								fmt = "yyyy-MM-ddTHH:mm:ss.fff";
								break;
							case DateTimeKind.Utc:
								break;
							case DateTimeKind.Local:
								dt = dt.ToLocalTime();
								break;
						}
						writer.WriteStringValue(dt.ToString(fmt));
						break;
					default:
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