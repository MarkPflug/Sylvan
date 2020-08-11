using System;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	// Adapts an IDataReader into a DbDataReader.
	sealed class IDataReaderAdpater : DbDataReader
	{
		readonly IDataReader dr;

		public IDataReaderAdpater(IDataReader dr)
		{
			if (dr == null) throw new ArgumentNullException(nameof(dr));
			this.dr = dr;
		}

		public override object this[int ordinal] => this.dr[ordinal];

		public override object this[string name] => this.dr[name];

		public override int Depth => this.dr.Depth;

		public override int FieldCount => this.dr.FieldCount;

		public override bool HasRows => true;

		public override bool IsClosed => this.dr.IsClosed;

		public override int RecordsAffected => this.dr.RecordsAffected;

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
			while (this.Read())
				yield return this;
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

		public override string GetString(int ordinal)
		{
			return this.dr.GetString(ordinal);
		}

		public override object GetValue(int ordinal)
		{
			return this.dr.GetValue(ordinal);
		}

		public override int GetValues(object[] values)
		{
			return this.dr.GetValues(values);
		}

		public override bool IsDBNull(int ordinal)
		{
			return this.dr.IsDBNull(ordinal);
		}

		public override bool NextResult()
		{
			return this.dr.NextResult();
		}

		public override bool Read()
		{
			return this.dr.Read();
		}
	}
}
