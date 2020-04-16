﻿using System;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	public sealed class CsvDataWriter
	{
		readonly CsvWriter writer;		
		
		public CsvDataWriter(TextWriter writer, CsvWriterOptions? options = null)
		{
			if (writer == null) throw new ArgumentNullException(nameof(writer));
			if (options != null)
			{
				options.Validate();
			}
			else
			{
				options = CsvWriterOptions.Default;
			}

			this.writer = new CsvWriter(writer, options);
		}

		class FieldInfo
		{
			public bool allowNull;
			public TypeCode type;
		}

		public async Task WriteAsync(DbDataReader reader, CancellationToken cancel = default)
		{
			var c = reader.FieldCount;
			var fieldTypes = new FieldInfo[c];

			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

			for (int i = 0; i < c; i++)
			{
				var type = reader.GetFieldType(i);
				var typeCode = Type.GetTypeCode(type);
				fieldTypes[i] =
					new FieldInfo
					{
						allowNull = schema?[i].AllowDBNull ?? true,
						type = typeCode
					};
			}
			
			for (int i = 0; i < c; i++)
			{
				var header = reader.GetName(i);
				await writer.WriteFieldAsync(header);
			}
			await writer.EndRecordAsync();

			cancel.ThrowIfCancellationRequested();
			while (await reader.ReadAsync(cancel))
			{
				for (int i = 0; i < c; i++)
				{
					var typeCode = fieldTypes[i].type;
					var allowNull = fieldTypes[i].allowNull;

					if (allowNull && reader.IsDBNull(i)) // TODO: async?
					{
						await writer.WriteFieldAsync("");
						break;
					}
					long intVal;
					string? str;
						
					switch (typeCode)
					{
						case TypeCode.Boolean:
							str = reader.GetBoolean(i) ? "true" : "false";
							goto str;
						case TypeCode.String:
							str = reader.GetString(i);
							goto str;
						case TypeCode.Byte:
							intVal = reader.GetByte(i);
							goto intVal;
						case TypeCode.Int16:
							intVal = reader.GetInt16(i);
							goto intVal;
						case TypeCode.Int32:
							intVal = reader.GetInt32(i);
							goto intVal;
						case TypeCode.Int64:
							intVal = reader.GetInt64(i);
						intVal:
							await writer.WriteFieldAsync(intVal);
							break;
						case TypeCode.DateTime:
							var dateVal = reader.GetDateTime(i);
							await writer.WriteFieldAsync(dateVal);
							break;
						case TypeCode.Single:
							var floatVal = reader.GetFloat(i);
							await writer.WriteFieldAsync(floatVal);
							break;
						case TypeCode.Double:
							var doubleVal = reader.GetDouble(i);
							await writer.WriteFieldAsync(doubleVal);
							break;
						case TypeCode.Empty:
						case TypeCode.DBNull:
							await writer.WriteFieldAsync("");
							break;
						default:
							str = reader.GetValue(i)?.ToString() ?? "";
						str:
							await writer.WriteFieldAsync(str);
							break;
					}
				}

				await writer.EndRecordAsync();

				cancel.ThrowIfCancellationRequested();
			}
			// flush any pending data on the way out.
			await writer.FlushAsync();			
		}


		public void Write(DbDataReader reader)
		{
			var c = reader.FieldCount;
			var fieldTypes = new FieldInfo[c];

			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

			for (int i = 0; i < c; i++)
			{
				var type = reader.GetFieldType(i);
				var typeCode = Type.GetTypeCode(type);
				fieldTypes[i] =
					new FieldInfo
					{
						allowNull = schema?[i].AllowDBNull ?? true,
						type = typeCode
					};
			}

			for (int i = 0; i < c; i++)
			{
				var header = reader.GetName(i);
				writer.WriteField(header);
			}
			writer.EndRecord();

			while (reader.Read())
			{
				for (int i = 0; i < c; i++)
				{
					var typeCode = fieldTypes[i].type;
					var allowNull = fieldTypes[i].allowNull;

					if (allowNull && reader.IsDBNull(i)) // TODO: async?
					{
						writer.WriteField("");
						break;
					}
					long intVal;
					string? str;

					switch (typeCode)
					{
						case TypeCode.Boolean:
							str = reader.GetBoolean(i) ? "true" : "false";
							goto str;
						case TypeCode.String:
							str = reader.GetString(i);
							goto str;
						case TypeCode.Byte:
							intVal = reader.GetByte(i);
							goto intVal;
						case TypeCode.Int16:
							intVal = reader.GetInt16(i);
							goto intVal;
						case TypeCode.Int32:
							intVal = reader.GetInt32(i);
							goto intVal;
						case TypeCode.Int64:
							intVal = reader.GetInt64(i);
						intVal:
							writer.WriteField(intVal);
							break;
						case TypeCode.DateTime:
							var dateVal = reader.GetDateTime(i);
							writer.WriteField(dateVal);
							break;
						case TypeCode.Single:
							var floatVal = reader.GetFloat(i);
							writer.WriteField(floatVal);
							break;
						case TypeCode.Double:
							var doubleVal = reader.GetDouble(i);
							writer.WriteField(doubleVal);
							break;
						case TypeCode.Empty:
						case TypeCode.DBNull:
							writer.WriteField("");
							break;
						default:
							str = reader.GetValue(i)?.ToString() ?? "";
						str:
							writer.WriteField(str);
							break;
					}
				}

				writer.EndRecord();
			}
			// flush any pending data on the way out.
			writer.Flush();
		}

		enum FieldState
		{
			Start,
			Done,
		}
	}
}
