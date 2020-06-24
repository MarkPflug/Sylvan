using System;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Writes data from a DbDataReader as delimited values to a TextWriter.
	/// </summary>
	public sealed class CsvDataWriter
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

		enum FieldState
		{
			Start,
			Done,
		}

		readonly CsvWriter writer;
		bool disposedValue;

		/// <summary>
		/// Creates a new CsvDataWriter.
		/// </summary>
		/// <param name="writer">The TextWriter to receive the delimited data.</param>
		/// <param name="options">The options used to configure the writer, or null to use the defaults.</param>
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

		const int Base64EncSize = 3 * 256; // must be a multiple of 3.

		/// <summary>
		/// Asynchronously writes delimited data.
		/// </summary>
		/// <param name="reader">The DbDataReader to be written.</param>
		/// <param name="cancel">A cancellation token to cancel the asynchronous operation.</param>
		/// <returns>A task representing the asynchronous write operation.</returns>
		public async Task WriteAsync(DbDataReader reader, CancellationToken cancel = default)
		{
			var c = reader.FieldCount;
			var fieldTypes = new FieldInfo[c];

			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

			byte[]? dataBuffer = null;

			for (int i = 0; i < c; i++)
			{
				var type = reader.GetFieldType(i);
				var allowNull = schema?[i].AllowDBNull ?? true;
				fieldTypes[i] = new FieldInfo(allowNull, type);
			}

			for (int i = 0; i < c; i++)
			{
				var header = reader.GetName(i);
				await writer.WriteFieldAsync(header);
			}
			await writer.EndRecordAsync();
			int row = 0;
			cancel.ThrowIfCancellationRequested();
			while (await reader.ReadAsync(cancel))
			{
				row++;
				int i = 0; // field
				try
				{
					for (; i < c; i++)
					{
						var type = fieldTypes[i].type;
						var typeCode = fieldTypes[i].typeCode;
						var allowNull = fieldTypes[i].allowNull;

						if (allowNull && await reader.IsDBNullAsync(i))
						{
							await writer.WriteFieldAsync("");
							continue;
						}
						int intVal;
						string? str;

						switch (typeCode)
						{
							case TypeCode.Boolean:
								var boolVal = reader.GetBoolean(i);
								await writer.WriteFieldAsync(boolVal);
								break;
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
							intVal:
								await writer.WriteFieldAsync(intVal);
								break;
							case TypeCode.Int64:
								var longVal = reader.GetInt64(i);
								await writer.WriteFieldAsync(longVal);
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
								if (type == typeof(byte[]))
								{
									if (dataBuffer == null)
									{
										dataBuffer = new byte[Base64EncSize];
									}
									var idx = 0;
									await writer.StartBinaryFieldAsync();
									int len = 0;
									while ((len = (int)reader.GetBytes(i, idx, dataBuffer, 0, Base64EncSize)) != 0)
									{
										writer.ContinueBinaryField(dataBuffer, len);
										idx += len;
									}
									break;
								}
								if (type == typeof(Guid))
								{
									var guid = reader.GetGuid(i);
									await writer.WriteFieldAsync(guid);
									break;
								}
								str = reader.GetValue(i)?.ToString() ?? "";
							str:
								await writer.WriteFieldAsync(str);
								break;
						}
					}
				}
				catch (ArgumentOutOfRangeException e)
				{
					throw new CsvRecordTooLargeException(row, i, null, e);
				}

				await writer.EndRecordAsync();

				cancel.ThrowIfCancellationRequested();
			}
			// flush any pending data on the way out.
			await writer.FlushAsync();
		}

		/// <summary>
		/// Writes delimited data to the output.
		/// </summary>
		/// <param name="reader">The DbDataReader to be written.</param>
		public void Write(DbDataReader reader)
		{
			var c = reader.FieldCount;
			var fieldTypes = new FieldInfo[c];

			byte[]? dataBuffer = null;

			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

			for (int i = 0; i < c; i++)
			{
				var type = reader.GetFieldType(i);
				var allowNull = schema?[i].AllowDBNull ?? true;
				fieldTypes[i] = new FieldInfo(allowNull, type);
			}

			for (int i = 0; i < c; i++)
			{
				var header = reader.GetName(i);
				writer.WriteField(header);
			}
			writer.EndRecord();
			int row = 0;
			while (reader.Read())
			{
				row++;
				int i = 0; // field
				try
				{
					for (; i < c; i++)
					{
						var allowNull = fieldTypes[i].allowNull;

						if (allowNull && reader.IsDBNull(i)) // TODO: async?
						{
							writer.WriteField("");
							continue;
						}


						var typeCode = fieldTypes[i].typeCode;
						int intVal;
						string? str;

						switch (typeCode)
						{
							case TypeCode.Boolean:
								var boolVal = reader.GetBoolean(i);
								writer.WriteField(boolVal);
								break;
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
							intVal:
								writer.WriteField(intVal);
								break;
							case TypeCode.Int64:
								var longVal = reader.GetInt64(i);
								writer.WriteField(longVal);
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
								var type = fieldTypes[i].type;
								if (type == typeof(byte[]))
								{
									if (dataBuffer == null)
									{
										dataBuffer = new byte[Base64EncSize];
									}
									var idx = 0;
									writer.StartBinaryFieldAsync().Wait();
									int len = 0;
									while ((len = (int)reader.GetBytes(i, idx, dataBuffer, 0, Base64EncSize)) != 0)
									{
										writer.ContinueBinaryField(dataBuffer, len);
										idx += len;
									}
									break;
								}
								if (type == typeof(Guid))
								{
									var guid = reader.GetGuid(i);
									writer.WriteField(guid);
									break;
								}

								str = reader.GetValue(i)?.ToString() ?? "";
							str:
								writer.WriteField(str);
								break;
						}
					}
				}
				catch (ArgumentOutOfRangeException e)
				{
					throw new CsvRecordTooLargeException(row, i, null, e);
				}

				writer.EndRecord();
			}
			// flush any pending data on the way out.
			((IDisposable)this.writer).Dispose();
		}

		//void Close()
		//{
		//	writer.Flush();
		//}

		private void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					((IDisposable)this.writer).Dispose();
				}

				disposedValue = true;
			}
		}

		void IDisposable.Dispose()
		{
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}

#if NETSTANDARD2_1
		ValueTask IAsyncDisposable.DisposeAsync()
		{
			return ((IAsyncDisposable)this.writer).DisposeAsync();
		}
#endif

	}
}
