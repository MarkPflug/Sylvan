using System;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;

namespace Sylvan.Data
{
	public abstract partial class DataReaderAdapter : DbDataReader, IDbColumnSchemaGenerator
	{
		DbDataReader dr;

		protected DbDataReader Reader => dr;

		public DataReaderAdapter(DbDataReader dr)
		{
			this.dr = dr;
		}

		public override object this[int ordinal] => dr[ordinal];

		public override object this[string name] => dr[name];

		public override int Depth => dr.Depth;

		public override int FieldCount => dr.FieldCount;

		public override bool HasRows => dr.HasRows;

		public override bool IsClosed => dr.IsClosed;

		public override int RecordsAffected => dr.RecordsAffected;

		public override bool GetBoolean(int ordinal)
		{
			return dr.GetBoolean(ordinal);
		}

		public override byte GetByte(int ordinal)
		{
			return dr.GetByte(ordinal);
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			return dr.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
		}

		public override char GetChar(int ordinal)
		{
			return dr.GetChar(ordinal);
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			return dr.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
		}

		public override string GetDataTypeName(int ordinal)
		{
			return dr.GetDataTypeName(ordinal);
		}

		public override DateTime GetDateTime(int ordinal)
		{
			return dr.GetDateTime(ordinal);
		}

		public override decimal GetDecimal(int ordinal)
		{
			return dr.GetDecimal(ordinal);
		}

		public override double GetDouble(int ordinal)
		{
			return dr.GetDouble(ordinal);
		}

		public override IEnumerator GetEnumerator()
		{
			return ((IEnumerable)dr).GetEnumerator();
		}

		public override Type GetFieldType(int ordinal)
		{
			return dr.GetFieldType(ordinal);
		}

		public override float GetFloat(int ordinal)
		{
			return dr.GetFloat(ordinal);
		}

		public override Guid GetGuid(int ordinal)
		{
			return dr.GetGuid(ordinal);
		}

		public override short GetInt16(int ordinal)
		{
			return dr.GetInt16(ordinal);
		}

		public override int GetInt32(int ordinal)
		{
			return dr.GetInt32(ordinal);
		}

		public override long GetInt64(int ordinal)
		{
			return dr.GetInt64(ordinal);
		}

		public override string GetName(int ordinal)
		{
			return dr.GetName(ordinal);
		}

		public override int GetOrdinal(string name)
		{
			return dr.GetOrdinal(name);
		}

		public override string? GetString(int ordinal)
		{
			return dr.GetString(ordinal);
		}

		public override object? GetValue(int ordinal)
		{
			if (this.IsDBNull(ordinal))
			{
				return null;
			}
			var type = this.GetFieldType(ordinal);

			switch (Type.GetTypeCode(type))
			{
				case TypeCode.Boolean:
					return this.GetBoolean(ordinal);
				case TypeCode.Byte:
					return this.GetByte(ordinal);
				case TypeCode.Char:
					return this.GetChar(ordinal);
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
					if (type == typeof(Guid))
					{
						return this.GetGuid(ordinal);
					}
					// otherwise resort to strings?
					return this.GetString(ordinal);
			}
		}

		public override Stream GetStream(int ordinal)
		{
			return new DbDataReaderStream(this, ordinal);
		}

		public override TextReader GetTextReader(int ordinal)
		{
			return new DbDataReaderTextReader(this, ordinal);
		}

		public override int GetValues(object?[] values)
		{
			if (values == null) throw new ArgumentNullException(nameof(values));
			var count = Math.Min(this.FieldCount, values.Length);
			for (int i = 0; i < count; i++)
			{
				values[i] = GetValue(i);
			}
			return count;
		}

		public override bool IsDBNull(int ordinal)
		{
			return dr.IsDBNull(ordinal);
		}

		public override T GetFieldValue<T>(int ordinal)
		{
			return dr.GetFieldValue<T>(ordinal);
		}

		public override bool NextResult()
		{
			return dr.NextResult();
		}

		public override bool Read()
		{
			return dr.Read();
		}

		public override DataTable GetSchemaTable()
		{
			return dr.GetSchemaTable();
		}

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			return dr.GetColumnSchema();
		}
	}
}