using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.XBase
{
	enum XBaseVersion : byte
	{
		FoxBase = 0x02,
		FoxBasePlusNoMemo = 0x03,
		VisualFoxPro = 0x30,
		VisualFoxProAutoIncrement = 0x31,
		VisualFoxProVarField = 0x32,
		DBase4SqlTableFiles = 0x43,
		DBase4SqlSystemFiles = 0x63,
		FoxBasePlusMemo = 0x83,
		DBase4Memo = 0x8b,
		DBase4SqlTableFilesMemo = 0xcb,
		FoxProMemo = 0xf5,
		FoxBaseEx = 0xfb,
	}

	[Flags]
	enum ColumnFlags
	{
		None = 0x00,
		SystemColumn = 0x01,
		Nullable = 0x02,
		Binary = 0x03,
		AutoIncrementing = 0x0c,
	}

	public sealed partial class XBaseDataReader : DbDataReader, IDbColumnSchemaGenerator
	{
		static Dictionary<ushort, ushort> CodePageMap;

		class XBaseColumn : DbColumn
		{
			internal int offset;
			internal int length;
			internal int decimalCount;

			internal int nullFlagIdx;
			internal int varFlagIdx;

			internal DataAccessor accessor;

			internal XBaseType DBaseDataType
			{
				get;
			}

			public XBaseColumn(int ordinal, string name, int offset, int length, XBaseType type, int decimalCount, bool isNullable, bool isSystem)
			{
				this.ColumnOrdinal = ordinal;
				this.ColumnName = name;
				this.offset = offset;
				this.length = length;
				this.ColumnSize = length;
				this.DBaseDataType = type;
				this.DataType = GetType(type);
				this.DataTypeName = type.ToString();
				this.decimalCount = decimalCount;
				
				this.IsHidden = isSystem;

				var acc = GetAccessor(type);
				this.accessor = isNullable ? new FoxProNullAccessor(acc) : acc;
				this.AllowDBNull = accessor.CanBeNull;
			}

			DataAccessor GetAccessor(XBaseType type)
			{
				switch (type)
				{
					case XBaseType.Character:
						return CharacterAccessor.Instance;
					case XBaseType.VarChar:
						return VarCharAccessor.Instance;
					case XBaseType.VarBinary:
						return VarBinaryAccessor.Instance;
					case XBaseType.Currency:
						return CurrencyAccessor.Instance;
					case XBaseType.Float:
					case XBaseType.Numeric:
						return NumericAccessor.Instance;
					case XBaseType.Double:
						return DoubleAccessor.Instance;
					case XBaseType.Integer:
						return Int32Accessor.Instance;
					case XBaseType.Logical:
						return BooleanAccessor.Instance;
					case XBaseType.Date:
						return DateAccessor.Instance;
					case XBaseType.DateTime:
						return DateTimeAccessor.Instance;
					case XBaseType.General:
					case XBaseType.Blob:
						return BlobAccessor.Instance;
					case XBaseType.Memo:
						return MemoAccessor.Instance;
					case XBaseType.NullFlags:
						return NullFlagsAccessor.Instance;
					default:
						return UnknownAccessor.Instance;
						//throw new NotSupportedException();
				}
			}

			static Type GetType(XBaseType type)
			{
				switch (type)
				{
					case XBaseType.Character:
						return typeof(string);
					case XBaseType.Currency:
					case XBaseType.Float:
					case XBaseType.Numeric:
						return typeof(decimal);
					case XBaseType.Double:
						return typeof(double);
					case XBaseType.Integer:
						//case DBaseColumnType.AutoIncrement:
						return typeof(int);
					case XBaseType.Logical:
						return typeof(bool);
					case XBaseType.Memo:
					case XBaseType.VarChar:
						return typeof(string);
					case XBaseType.Date:
					case XBaseType.DateTime:
						return typeof(DateTime);
					case XBaseType.General:
					case XBaseType.VarBinary:
					case XBaseType.Blob:
						return typeof(byte[]);
					default:
						return typeof(object);
				}
			}

			public override string ToString()
			{
				return $"{ColumnName} {length} {DBaseDataType} {decimalCount}";
			}
		}

		string cachedRecord;
		int cachedRecordRow;
		int cachedRecordOrdinal;

		Stream stream;
		Stream? memoStream;
		XBaseColumn[] columns;
		XBaseColumn? nullFlagsColumn;

		bool isClosed;
		int recordIdx;
		int recordLength;
		byte[] recordBuffer;
		byte[] memoBuffer;

		int memoBlockSize;

		Encoding encoding;
		int recordCount;

		XBaseVersion version;
		public DateTime ModifiedDate { get; private set; }
		public override int VisibleFieldCount => columns.Length;

		[Flags]
		enum DBaseFileFlags
		{
			HasIndex = 0x01,
			HasMemo = 0x02,
			HasDatabase = 0x04,
		}

		[Flags]
		enum DBaseColumnFlags
		{
			System = 0x01,
			Nullable = 0x02,
			Binary = 0x04,
			AutoIncrementing = 0x0c,
		}


		public static XBaseDataReader Create(Stream stream)
		{
			return CreateAsync(stream, null).GetAwaiter().GetResult();
		}

		public static XBaseDataReader Create(Stream stream, Stream? memoStream)
		{
			return CreateAsync(stream, memoStream).GetAwaiter().GetResult();
		}

		public static Task<XBaseDataReader> CreateAsync(Stream stream, XBaseDataReaderOptions? options = null)
		{
			return CreateAsync(stream, null, options);
		}

		public static async Task<XBaseDataReader> CreateAsync(Stream stream, Stream? memoStream, XBaseDataReaderOptions? options = null)
		{
			options = options ?? XBaseDataReaderOptions.Default;
			var reader = new XBaseDataReader(stream, memoStream);
			await reader.InitializeAsync(options);
			return reader;
		}

		XBaseDataReader(Stream stream, Stream? memoStream)
		{
			this.stream = stream;
			this.memoStream = memoStream;
			this.columns = Array.Empty<XBaseColumn>();
			this.encoding = Encoding.Default;
			this.recordBuffer = Array.Empty<byte>();
			this.memoBuffer = Array.Empty<byte>();
			this.cachedRecord = string.Empty;
		}

		async Task InitializeAsync(XBaseDataReaderOptions options)
		{
			var buffer = new byte[0x20];
			var p = 0;
			var len = await stream.ReadAsync(buffer, 0, buffer.Length);
			if (len != 0x20)
				throw new InvalidDataException();

			p += len;

			var version = (XBaseVersion)buffer[0];
			var date = new DateTime(1900 + buffer[1], buffer[2], buffer[3], 0, 0, 0, DateTimeKind.Utc);

			this.recordCount = BitConverter.ToInt32(buffer, 4);
			var headerLength = BitConverter.ToInt16(buffer, 8);
			this.recordLength = BitConverter.ToInt16(buffer, 10);
			this.recordBuffer = new byte[recordLength];

			var flags = (DBaseFileFlags)buffer[0x1c];
			var langId = buffer[0x1d];

			if(options.Encoding != null)
			{
				this.encoding = options.Encoding;
			} 
			else
			{
				if (langId == 0)
				{
					this.encoding = Encoding.Default;
				}
				else
				{
					if (CodePageMap.TryGetValue(langId, out var codePage))
					{
						this.encoding = Encoding.GetEncoding(codePage);
					}
					else
					{
						// TODO: consider adding an option to fall-back to system default encoding?
						throw new EncodingNotSupportedException(langId);
					}
				}
			}			

			var fields = new List<XBaseColumn>(16);

			if (recordCount < 0)
				throw new InvalidDataException();

			var flagIdx = 0;
			int fieldOffset = 1;
			for (int i = 0; i < 128; i++)
			{
				len = await stream.ReadAsync(buffer, 0, 1);
				if (len != 1)
					throw new InvalidDataException();
				p += len;

				if (buffer[0] == '\x0d')
				{
					break;
				}
				len = await stream.ReadAsync(buffer, 1, 0x1f);
				if (len != 0x1f)
					throw new InvalidDataException();
				p += len;
				var name = ReadZString(buffer, 0, 10);
				var type = (XBaseType)buffer[0x0b];
				var decimalCount = (byte)0;
				var fieldLength = 0;

				if (type == XBaseType.Character)
				{
					fieldLength = BitConverter.ToInt16(buffer, 0x10);
				}
				else
				{
					fieldLength = buffer[0x10];
					decimalCount = buffer[0x11];
				}

				var colFlags = (DBaseColumnFlags)buffer[0x12];

				var isAutoIncrement = colFlags.HasFlag(DBaseColumnFlags.AutoIncrementing);
				var nullable = colFlags.HasFlag(DBaseColumnFlags.Nullable);
				var system = colFlags.HasFlag(DBaseColumnFlags.System);

				if (isAutoIncrement)
				{
					var next = BitConverter.ToInt32(buffer, 0x13);
					var step = buffer[0x17];
				}

				if (system)
				{
					;
				}

				var field = new XBaseColumn(i, name, fieldOffset, fieldLength, type, decimalCount, nullable, system);

				switch (field.DBaseDataType)
				{
					case XBaseType.VarBinary:
					case XBaseType.VarChar:
						field.varFlagIdx = flagIdx++;
						break;
				}

				if (nullable)
				{
					field.nullFlagIdx = flagIdx++;
				}

				fieldOffset += fieldLength;
				fields.Add(field);
			}

			if (memoStream != null)
			{
				this.memoBuffer = new byte[0x200];
				len = memoStream.Read(memoBuffer, 0, memoBuffer.Length);
				if (len != 0x200)
				{
					throw new InvalidDataException();
				}
				this.memoBlockSize = IPAddress.HostToNetworkOrder(BitConverter.ToInt16(memoBuffer, 6));

				len = memoStream.Read(memoBuffer, 0, memoBuffer.Length);
			}

			this.columns = fields.Where(f => f.IsHidden == false).ToArray();
			this.nullFlagsColumn = fields.FirstOrDefault(f => f.IsHidden == true && f.ColumnName == "_NullFlags");

			var remainingHeaderLength = headerLength - p;

			// we avoid seek operations and use forward only reads
			// to support reading directly out of non-seekable streams
			// this is to support reading data directly out of zip files
			// to support GIS shape data.
			while (remainingHeaderLength > 0)
			{
				var readCount = Math.Min(remainingHeaderLength, buffer.Length);
				len = await stream.ReadAsync(buffer, 0, readCount);
				if (len != readCount)
					throw new InvalidDataException();
				remainingHeaderLength -= len;
			}

			this.ModifiedDate = date;
			this.version = version;
		}




		string ReadZString(byte[] buffer, int offset, int maxLength)
		{
			int i;
			for (i = 0; i < maxLength; i++)
			{
				if (buffer[offset + i] == '\0')
				{
					break;
				}
			}
			return encoding.GetString(buffer, offset, i);
		}

		public override object this[int ordinal] => this.GetValue(ordinal);

		public override object this[string name] => this.GetValue(this.GetOrdinal(name));

		public override int Depth => 0;

		public override int FieldCount => this.columns.Length;

		public override bool HasRows => recordCount > 0;

		public override bool IsClosed => isClosed;

		public override int RecordsAffected => recordCount;

		public override bool GetBoolean(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.accessor.GetBoolean(this, ordinal);
		}

		public override byte GetByte(int ordinal)
		{
			throw new NotSupportedException();
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
		{
			if (dataOffset > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(dataOffset));
			if (buffer == null) throw new ArgumentNullException(nameof(buffer));

			var col = this.columns[ordinal];
			return col.accessor.GetBytes(this, ordinal, (int)dataOffset, buffer, bufferOffset, length);
		}

		public override char GetChar(int ordinal)
		{
			throw new NotSupportedException();
		}

		public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
		{
			if (dataOffset > int.MaxValue)
				throw new ArgumentOutOfRangeException(nameof(dataOffset));
			if (buffer == null)
				throw new ArgumentNullException(nameof(buffer));

			var col = this.columns[ordinal];
			return col.accessor.GetChars(this, ordinal, (int)dataOffset, buffer, bufferOffset, length);
		}



		public override string GetDataTypeName(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.DataTypeName?.ToString() ?? col.DBaseDataType.ToString();
		}

		public override DateTime GetDateTime(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.accessor.GetDateTime(this, ordinal);
		}

		public override decimal GetDecimal(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.accessor.GetDecimal(this, ordinal);
		}

		public override double GetDouble(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.accessor.GetDouble(this, ordinal);
		}

		public override IEnumerator GetEnumerator()
		{
			return new DbEnumerator(this);
		}

		public override Type GetFieldType(int ordinal)
		{
			return this.columns[ordinal].DataType!;
		}

		public override float GetFloat(int ordinal)
		{
			throw new NotSupportedException();
		}

		public override Guid GetGuid(int ordinal)
		{
			throw new NotSupportedException();
		}

		public override short GetInt16(int ordinal)
		{
			return (short)GetInt32(ordinal);
		}

		public override int GetInt32(int ordinal)
		{
			var col = columns[ordinal];
			return col.accessor.GetInt32(this, ordinal);
		}

		public override long GetInt64(int ordinal)
		{
			return GetInt32(ordinal);
		}

		public override string GetName(int ordinal)
		{
			return this.columns[ordinal].ColumnName;
		}

		public override int GetOrdinal(string name)
		{
			for (int i = 0; i < columns.Length; i++)
			{
				if (StringComparer.OrdinalIgnoreCase.Equals(columns[i].ColumnName, name))
					return i;
			}
			return -1;
		}

		public override string GetString(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.accessor.GetString(this, ordinal);
		}

		public override Stream GetStream(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.accessor.GetStream(this, ordinal);
		}

		public override TextReader GetTextReader(int ordinal)
		{
			var col = this.columns[ordinal];
			return col.accessor.GetTextReader(this, ordinal);
		}

		void ThrowIfOutOfRange(int ordinal)
		{
			if ((uint)ordinal >= (uint)columns.Length)
			{
				throw new ArgumentOutOfRangeException(nameof(ordinal));
			}
		}

		public override object GetValue(int ordinal)
		{
			ThrowIfOutOfRange(ordinal);

			if (columns[ordinal].AllowDBNull != false && this.IsDBNull(ordinal))
			{
				return DBNull.Value;
			}
			var type = this.GetFieldType(ordinal);

			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return this.GetBoolean(ordinal);
				case TypeCode.Char:
					return this.GetChar(ordinal);
				case TypeCode.Byte:
					return this.GetByte(ordinal);
				case TypeCode.Int16:
					return this.GetInt16(ordinal);
				case TypeCode.Int32:
					return this.GetInt32(ordinal);
				case TypeCode.Int64:
					return this.GetInt64(ordinal);
				case TypeCode.Single:
					return this.GetFloat(ordinal);
				case TypeCode.Double:
					return this.GetDouble(ordinal);
				case TypeCode.Decimal:
					return this.GetDecimal(ordinal);
				case TypeCode.DateTime:
					return this.GetDateTime(ordinal);
				case TypeCode.String:
					return this.GetString(ordinal);
				default:
					return this.GetString(ordinal);
			}
		}

#if DEBUG
		byte[] GetRecordBytes(int ordinal)
		{
			var col = this.columns[ordinal];
			var buffer = new byte[col.length];
			Buffer.BlockCopy(this.recordBuffer, col.offset, buffer, 0, col.length);
			return buffer;
		}

		char[] GetRecordChars(int ordinal)
		{
			var col = this.columns[ordinal];
			var buffer = new char[col.length];
			for (int i = 0; i < col.length; i++)
			{
				buffer[i] = (char)this.recordBuffer[col.offset + i];
			}
			return buffer;
		}

		string GetRecordString(int ordinal)
		{
			var col = this.columns[ordinal];
			return Encoding.ASCII.GetString(this.recordBuffer, col.offset, col.length);
		}

		byte[] GetNullFlags()
		{
			if (this.nullFlagsColumn != null)
			{
				var bytes = new byte[nullFlagsColumn.length];
				Buffer.BlockCopy(this.recordBuffer, nullFlagsColumn.offset, bytes, 0, nullFlagsColumn.length);
				return bytes;
			}
			return Array.Empty<byte>();
		}

		string GetNullFlagsStr()
		{
			if (this.nullFlagsColumn != null)
			{
				var bytes = new byte[nullFlagsColumn.length];
				Buffer.BlockCopy(this.recordBuffer, nullFlagsColumn.offset, bytes, 0, nullFlagsColumn.length);
				char[] cs = new char[bytes.Length * 8];
				var idx = 0;
				for (int i = 0; i < bytes.Length; i++)
				{
					var v = bytes[i];
					for (int b = 0; b < 8; b++)
					{
						cs[idx++] = ((v >> b) & 0x1) == 0x01 ? '1' : '0';
					}
				}
				return new string(cs);
			}
			return "";
		}
#endif

		public override int GetValues(object?[] values)
		{
			var l = Math.Min(columns.Length, values.Length);
			for (int i = 0; i < l; i++)
			{
				values[i] = GetValue(i);
			}
			return l;
		}

		public override bool IsDBNull(int ordinal)
		{
			var col = columns[ordinal];
			return col.accessor.IsDBNull(this, ordinal);
		}

		public override bool NextResult()
		{
			return false;
		}

		public override bool Read()
		{
			do
			{
				var recordLen = this.recordLength;
				if (recordIdx >= this.recordCount)
				{
					return false;
				}
				recordIdx++;
				var len = stream.Read(recordBuffer, 0, recordLen);
				if (len < recordLen)
				{
					throw new InvalidDataException();
				}
			}
			while (recordBuffer[0] != ' ');

			return true;
		}

		public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
		{
			do
			{
				var recordLen = this.recordLength;
				if (recordIdx >= this.recordCount)
				{
					return false;
				}
				recordIdx++;
				var len = await stream.ReadAsync(recordBuffer, 0, recordLen, cancellationToken);
				if (len < recordLen)
				{
					throw new InvalidDataException();
				}
				cancellationToken.ThrowIfCancellationRequested();
			}
			while (recordBuffer[0] != ' ');

			return true;
		}

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			return new ReadOnlyCollection<DbColumn>(columns);
		}

		public override DataTable GetSchemaTable()
		{
			return SchemaTable.GetSchemaTable(GetColumnSchema());
		}

		protected override void Dispose(bool disposing)
		{
			this.isClosed = true;
			if (disposing)
			{
				this.stream.Dispose();
			}
			base.Dispose(disposing);
		}
	}
}
