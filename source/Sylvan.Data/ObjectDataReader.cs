using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;

namespace Sylvan.Data
{
	public static class ObjectDataReader
	{
		public static ObjectDataReader<T> Create<T>(IEnumerable<T> data)
		{
			return new ObjectDataReader<T>(data.GetEnumerator());
		}
	}

	public sealed class ObjectDataReader<T> : DbDataReader, IDbColumnSchemaGenerator
	{
		class ColumnInfo : DbColumn
		{
			public object selector;
			public Func<T, object> valueSelector;
			public TypeCode typeCode;

			public ColumnInfo(int ordinal, string name, Type type, object selector, Func<T, Object> valueSelector)
			{
				this.selector = selector;
				this.valueSelector = valueSelector;
				this.typeCode = Type.GetTypeCode(type);
				this.ColumnOrdinal = ordinal;
				this.ColumnName = name;
				this.DataType = type;
				this.DataTypeName = this.DataType.Name;
#warning todo: handle Nullable<T>. Unwrap T and set allow null.
				this.AllowDBNull = this.DataType.IsValueType;
			}
		}

		readonly IEnumerator<T> enumerator;
		List<ColumnInfo> columns;
		bool isClosed;

		public ObjectDataReader(IEnumerator<T> enumerator)
		{
			this.enumerator = enumerator;
			this.columns = new List<ColumnInfo>();
			this.isClosed = false;
		}

		int c = 0;

		public void AddColumn<T0>(string name, Func<T, T0> func)
		{
			Func<T, object> valueSelector = item => func(item);
			this.columns.Add(new ColumnInfo(c++, name, typeof(T0), func, valueSelector));
		}

		public override bool IsClosed
		{
			get { return this.isClosed; }
		}

		public override void Close()
		{
			this.enumerator.Dispose();
			this.isClosed = true;
		}

		public override bool NextResult()
		{
			return false;
		}

		public override bool Read()
		{
			var result = this.enumerator.MoveNext();
			return result;
		}

		public override string GetName(int ordinal)
		{
			return this.columns[ordinal].ColumnName;
		}

		public override Type GetFieldType(int ordinal)
		{
			return this.columns[ordinal].DataType;
		}

		public override int GetOrdinal(String name)
		{
			for (int i = 0; i < this.columns.Count; i++)
				if (String.Compare(columns[i].ColumnName, name, false) == 0)
					return i;

			for (int i = 0; i < this.columns.Count; i++)
				if (String.Compare(columns[i].ColumnName, name, true) == 0)
					return i;

			throw new ArgumentOutOfRangeException(nameof(name));
		}

		public override bool GetBoolean(int ordinal)
		{
			return GetFieldValue<bool>(ordinal);
		}

		public override TValue GetFieldValue<TValue>(int ordinal)
		{
			var col = columns[ordinal];
			if (col.selector is Func<T, TValue> b)
			{
				return b(enumerator.Current);
			}
			throw new InvalidCastException();
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
			return this.columns[ordinal].DataTypeName;
		}

		public override DateTime GetDateTime(int ordinal)
		{
			return GetFieldValue<DateTime>(ordinal);
		}

		public override decimal GetDecimal(int ordinal)
		{
			return GetFieldValue<decimal>(ordinal);
		}

		public override double GetDouble(int ordinal)
		{
			return GetFieldValue<double>(ordinal);
		}

		public override IEnumerator GetEnumerator()
		{
			throw new NotImplementedException();
		}

		public override float GetFloat(int ordinal)
		{
			return GetFieldValue<float>(ordinal);
		}

		public override Guid GetGuid(int ordinal)
		{
			return GetFieldValue<Guid>(ordinal);
		}

		public override short GetInt16(int ordinal)
		{
			return GetFieldValue<short>(ordinal);
		}

		public override int GetInt32(int ordinal)
		{
			return GetFieldValue<int>(ordinal);
		}

		public override long GetInt64(int ordinal)
		{
			return GetFieldValue<long>(ordinal);
		}

		public override string GetString(int ordinal)
		{
			var col = columns[ordinal];
			var cur = enumerator.Current;
			if (col.selector is Func<T, string> s)
			{
				return s(cur);
			}
			return col.valueSelector(cur)?.ToString() ?? "";
		}

		public override object GetValue(int ordinal)
		{
			var value = this.columns[ordinal].valueSelector(enumerator.Current);
			return value;
		}

		public override int GetValues(object[] values)
		{
			throw new NotImplementedException();
		}

		public override bool IsDBNull(int ordinal)
		{
			return false;
		}

		public ReadOnlyCollection<DbColumn> GetColumnSchema()
		{
			return new ReadOnlyCollection<DbColumn>(this.columns.ToArray());
		}

		public override int FieldCount
		{
			get { return this.columns.Count; }
		}

		public override int Depth => 0;

		public override bool HasRows => true;

		public override int RecordsAffected => -1;

		public override object this[string name] => GetValue(this.GetOrdinal(name));

		public override object this[int ordinal] => this.GetValue(ordinal);
	}
}
