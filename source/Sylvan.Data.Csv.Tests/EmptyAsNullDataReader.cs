using System;
using System.Collections;
using System.Data.Common;

namespace Sylvan.Data.Csv
{
	// SqlBulkCopy won't convert empty strings, but requires that the string be null
	// when inserting into non-string columns. I've made the philosophical decision that
	// by default the CsvDataReader schema is non-null strings, so a missing field is
	// returned as an empty string. This adapter can be used to turn empty strings into
	// nulls to make SqlBulkCopy happy.
	class EmptyAsNullDataReader : DbDataReader
	{
		DbDataReader dr;

		public EmptyAsNullDataReader(DbDataReader dr)
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
			throw new NotImplementedException();
		}

		public override byte GetByte(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override char GetChar(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
		{
			throw new NotImplementedException();
		}

		public override string GetDataTypeName(int ordinal)
		{
			return dr.GetDataTypeName(ordinal);
		}

		public override DateTime GetDateTime(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override decimal GetDecimal(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override double GetDouble(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}

		public override Type GetFieldType(int ordinal)
		{
			return dr.GetFieldType(ordinal);
		}

		public override float GetFloat(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override Guid GetGuid(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override short GetInt16(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override int GetInt32(int ordinal)
		{
			throw new NotImplementedException();
		}

		public override long GetInt64(int ordinal)
		{
			throw new NotImplementedException();
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
			var str = dr.GetString(ordinal);
			return str?.Length == 0 ? null : str;
		}

		public override object GetValue(int ordinal)
		{
			return this.GetString(ordinal);
		}

		public override int GetValues(object[] values)
		{
			return dr.GetValues(values);
		}

		public override bool IsDBNull(int ordinal)
		{
			return dr.IsDBNull(ordinal);
		}

		public override bool NextResult()
		{
			return dr.NextResult();
		}

		public override bool Read()
		{
			return dr.Read();
		}
	}
}
