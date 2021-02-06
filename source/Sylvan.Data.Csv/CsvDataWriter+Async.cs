using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	partial class CsvDataWriter
	{
//		/// <summary>
//		/// Asynchronously writes delimited data.
//		/// </summary>
//		/// <param name="reader">The DbDataReader to be written.</param>
//		/// <param name="cancel">A cancellation token to cancel the asynchronous operation.</param>
//		/// <returns>A task representing the asynchronous write operation.</returns>
//		public async Task<long> WriteAsync(DbDataReader reader, CancellationToken cancel = default)
//		{
//			var c = reader.FieldCount;
//			var fieldTypes = new FieldInfo[c];

//			var schema = (reader as IDbColumnSchemaGenerator)?.GetColumnSchema();

//			char[] buffer = this.buffer;
//			int bufferSize = this.bufferSize;

//			for (int i = 0; i < c; i++)
//			{
//				var type = reader.GetFieldType(i);
//				var allowNull = schema?[i].AllowDBNull ?? true;
//				fieldTypes[i] = new FieldInfo(allowNull, type);
//			}

//			for (int i = 0; i < c; i++)
//			{
//				var header = reader.GetName(i);
//				var f = WriteField(header);
//				if(f)
//			}
//			await EndRecordAsync();
//			int row = 0;
//			cancel.ThrowIfCancellationRequested();
//			while (await reader.ReadAsync(cancel))
//			{
//				row++;
//				int i = 0; // field
//				try
//				{
//					for (; i < c; i++)
//					{
//						var type = fieldTypes[i].type;
//						var typeCode = fieldTypes[i].typeCode;
//						var allowNull = fieldTypes[i].allowNull;

//						if (allowNull && await reader.IsDBNullAsync(i))
//						{
//							await WriteFieldAsync();
//							continue;
//						}
//						int intVal;
//						string? str;

//						switch (typeCode)
//						{
//							case TypeCode.Boolean:
//								var boolVal = reader.GetBoolean(i);
//								await WriteFieldAsync(boolVal);
//								break;
//							case TypeCode.String:
//								str = reader.GetString(i);
//								goto str;
//							case TypeCode.Byte:
//								intVal = reader.GetByte(i);
//								goto intVal;
//							case TypeCode.Int16:
//								intVal = reader.GetInt16(i);
//								goto intVal;
//							case TypeCode.Int32:
//								intVal = reader.GetInt32(i);
//							intVal:
//								await WriteFieldAsync(intVal);
//								break;
//							case TypeCode.Int64:
//								var longVal = reader.GetInt64(i);
//								await WriteFieldAsync(longVal);
//								break;
//							case TypeCode.DateTime:
//								var dateVal = reader.GetDateTime(i);
//								await WriteFieldAsync(dateVal);
//								break;
//							case TypeCode.Single:
//								var floatVal = reader.GetFloat(i);
//								await WriteFieldAsync(floatVal);
//								break;
//							case TypeCode.Double:
//								var doubleVal = reader.GetDouble(i);
//								await WriteFieldAsync(doubleVal);
//								break;
//							case TypeCode.Empty:
//							case TypeCode.DBNull:
//								await WriteFieldAsync();
//								break;
//							default:
//								if (type == typeof(byte[]))
//								{
//									if (dataBuffer == null)
//									{
//										dataBuffer = new byte[Base64EncSize];
//									}
//									var idx = 0;
//									await StartBinaryFieldAsync();
//									int len = 0;
//									while ((len = (int)reader.GetBytes(i, idx, dataBuffer, 0, Base64EncSize)) != 0)
//									{
//										ContinueBinaryField(dataBuffer, len);
//										idx += len;
//									}
//									break;
//								}
//								if (type == typeof(Guid))
//								{
//									var guid = reader.GetGuid(i);
//									await WriteFieldAsync(guid);
//									break;
//								}
//								str = reader.GetValue(i)?.ToString();
//							str:
//								await WriteFieldAsync(str);
//								break;
//						}
//					}
//				}
//				catch (ArgumentOutOfRangeException e)
//				{
//					throw new CsvRecordTooLargeException(row, i, null, e);
//				}

//				await EndRecordAsync();

//				cancel.ThrowIfCancellationRequested();
//			}
//			// flush any pending data on the way out.
//			// await writer.FlushAsync();
//			return row;
//		}

//		async Task StartBinaryFieldAsync()
//		{
//			AssertBinaryPrereq();
//			await FlushBufferAsync();
//			if (fieldIdx > 0)
//			{
//				buffer[pos++] = delimiter;
//			}
//			fieldIdx++;
//		}

//		async Task FlushBufferAsync()
//		{
//			if (this.pos == 0) return;
//			await writer.WriteAsync(buffer, 0, pos);
//			pos = 0;
//		}

//		/// <summary>
//		/// Asynchronously flushes any pending data to the output writer.
//		/// </summary>
//		/// <returns>A task representing the asynchronous operation.</returns>
//		public Task FlushAsync()
//		{
//			return FlushBufferAsync();
//		}

//#if NETSTANDARD2_1
//		ValueTask IAsyncDisposable.DisposeAsync()
//		{
//			GC.SuppressFinalize(this);
//			if (!disposedValue)
//			{
//				var task = ((IAsyncDisposable)this.writer).DisposeAsync();
//				disposedValue = true;
//				return task;
//			}
//			return new ValueTask(Task.CompletedTask);
//		}
//#endif

	}
}
