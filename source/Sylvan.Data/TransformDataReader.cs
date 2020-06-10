//using System;
//using System.Collections;
//using System.Data.Common;

//namespace Sylvan.Data
//{
//	public class TransformBuilder
//	{
//		public TransformBuilder Map(int sourceIdx, int targetIdx) {
//			return this;
//		}
//	}

//	class TransformDataReader : DbDataReader
//	{
//		readonly DbDataReader reader;

//		class Transform
//		{
//			// source is the inner reader, mapped to the target: this class.
//			//public string targetName;
//			//public string sourceName;
//			//public int targetIdx;
//			//public int sourceIdx;
//			//public Type mappedType;
//		}

//		//Transform[] transforms;
//		//bool isClosed;

//		public TransformDataReader(DbDataReader reader, Action<TransformBuilder> builder)
//		{
//			this.reader = reader;
//			throw new NotImplementedException();
//		}

//		public override object this[int ordinal] => this.GetValue(ordinal);

//		public override object this[string name] => this.GetValue(GetOrdinal(name));

//		public override int Depth => 0;

//		public override int FieldCount => 0;// transforms.Length;

//		public override bool HasRows => reader.HasRows;

//		public override bool IsClosed => IsClosed;

//		public override int RecordsAffected => reader.RecordsAffected;

//		public override bool GetBoolean(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override byte GetByte(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
//		{
//			throw new NotImplementedException();
//		}

//		public override char GetChar(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
//		{
//			throw new NotImplementedException();
//		}

//		public override string GetDataTypeName(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override DateTime GetDateTime(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override decimal GetDecimal(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override double GetDouble(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override IEnumerator GetEnumerator()
//		{
//			throw new NotImplementedException();
//		}

//		public override Type GetFieldType(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override float GetFloat(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override Guid GetGuid(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override short GetInt16(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override int GetInt32(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override long GetInt64(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override string GetName(int ordinal)
//		{
//			return transforms[ordinal].targetName;
//		}

//		public override int GetOrdinal(string name)
//		{
//			throw new NotImplementedException();
//		}

//		public override string GetString(int ordinal)
//		{
//			throw new NotImplementedException();
//		}

//		public override object GetValue(int ordinal)
//		{
//			return reader.IsDBNull(GetSourceOrdinal(ordinal));
//		}

//		public override int GetValues(object[] values)
//		{
//			throw new NotImplementedException();
//		}

//		public override bool IsDBNull(int ordinal)
//		{
//			return reader.IsDBNull(GetSourceOrdinal(ordinal));
//		}

//		int GetSourceOrdinal(int ordinal)
//		{
//			return transforms[ordinal].sourceIdx;
//		}

//		public override bool NextResult()
//		{
//			return this.reader.NextResult();
//		}

//		public override bool Read()
//		{
//			return this.reader.Read();
//		}
//	}
//}
