using System;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;

namespace Sylvan.Data;

/// <summary>
/// Defines a data column.
/// </summary>
public interface IDataColumn
{
	/// <summary>
	/// The column name.
	/// </summary>
	string Name { get; }

	/// <summary>
	/// The column data type.
	/// </summary>
	Type ColumnType { get; }

	/// <summary>
	/// Indicates if the column allows null values.
	/// </summary>
	bool AllowNull { get; }

	/// <summary>
	/// Determines if the column value is null.
	/// </summary>
	bool IsDbNull(DbDataReader reader);

	/// <summary>
	/// Gets the object value of the column.
	/// </summary>
	/// <param name="reader"></param>
	/// <returns></returns>
	object GetValue(DbDataReader reader);

	/// <summary>
	/// Gets the value of the column.
	/// </summary>
	T GetValue<T>(DbDataReader reader);

	/// <summary>
	/// Gets a range of data from the column.
	/// </summary>
	int GetData<T>(DbDataReader reader, T[]? buffer, long dataOffset, int bufferOffset, int length);
}

/// <summary>
/// Defines a custom data column.
/// </summary>
/// <typeparam name="T">The data type of the column.</typeparam>
public sealed class CustomDataColumn<T> : IDataColumn
{
	/// <inheritdoc/>
	public string Name { get; }
	/// <inheritdoc/>
	public Type ColumnType { get; }
	/// <inheritdoc/>
	public bool AllowNull { get; }

	/// <inheritdoc/>
	public bool IsDbNull(DbDataReader reader)
	{
		return
			this.AllowNull
			? GetValue(reader) == DBNull.Value
			: false;
	}

	/// <inheritdoc/>
	public TValue GetValue<TValue>(DbDataReader reader)
	{
		if (typeof(TValue) != typeof(T))
			throw new InvalidCastException();

		T t = valueSource(reader);
		if (t is TValue value)
		{
			return value;
		}
		else
		{
			throw new InvalidCastException();
		}
	}

	/// <inheritdoc/>
	public object GetValue(DbDataReader reader)
	{
		var obj = valueSource(reader);
		if (obj is null)
		{
			return DBNull.Value;
		}
		return obj;
	}

	/// <inheritdoc/>
	public int GetData<TData>(DbDataReader reader, TData[]? buffer, long dataOffset, int bufferOffset, int length)
	{
		var t = valueSource(reader);
		if (t is TData[] data)
		{
			var len = 0;
			if (buffer == null)
			{
				// passing a null buffer allows querying the length.
				len = data.Length;
			}
			else
			{
				len = Math.Min(data.Length - (int)dataOffset, length);
				Array.Copy(data, dataOffset, buffer, bufferOffset, len);
			}
			return len;
		}
		else
		{
			throw new InvalidCastException();
		}
	}

	readonly Func<DbDataReader, T> valueSource;

	/// <summary>
	/// Creates a new CustomDataColumn instance.
	/// </summary>
	public CustomDataColumn(string name, Func<DbDataReader, T> valueSource)
	{
		this.Name = name;
		var t = typeof(T);
		var nt = Nullable.GetUnderlyingType(t);
		this.ColumnType = nt ?? t;
		this.AllowNull = nt != null || t.IsByRef;
		this.valueSource = valueSource;
	}
}

sealed class DataReaderColumn : IDataColumn
{
	readonly DbDataReader reader;
	readonly int ordinal;
	readonly bool allowNull;

	public DataReaderColumn(DbDataReader reader, int ordinal, bool allowNull)
	{
		this.reader = reader;
		this.ordinal = ordinal;
		this.allowNull = allowNull;
	}

	public string Name => reader.GetName(ordinal);

	public Type ColumnType => reader.GetFieldType(ordinal);

	public bool AllowNull => allowNull;

	public int GetData<TData>(DbDataReader reader, TData[]? buffer, long dataOffset, int bufferOffset, int length)
	{
		throw new NotSupportedException();
	}

	public object GetValue(DbDataReader reader)
	{
		return this.reader.GetValue(ordinal);
	}

	public TValue GetValue<TValue>(DbDataReader reader)
	{
		return this.reader.GetFieldValue<TValue>(ordinal);
	}

	public bool IsDbNull(DbDataReader reader)
	{
		return this.reader.IsDBNull(ordinal);
	}
}

sealed class ExtendedDataReader : DbDataReader, IDbColumnSchemaGenerator
{
	readonly DbDataReader dr;
	readonly IDataColumn[] columns;
	readonly ReadOnlyCollection<DbColumn> schema;

	public ExtendedDataReader(DbDataReader dr, IDataColumn[] customColumns)
	{
		this.dr = dr;
		this.columns = new IDataColumn[dr.FieldCount + customColumns.Length];
		int i = 0;
		var s = dr.GetColumnSchema();
		DbColumn[] schema = new DbColumn[columns.Length];

		for (; i < dr.FieldCount; i++)
		{
			columns[i] = new DataReaderColumn(dr, i, s[i].AllowDBNull != false);
			schema[i] = s[i];
		}

		for (int c = 0; c < customColumns.Length; c++, i++)
		{
			var col = customColumns[c];
			columns[i] = col;
			schema[i] = new CustomDbColumn(col, i);
		}
		this.schema = new ReadOnlyCollection<DbColumn>(schema);
	}

	class CustomDbColumn : DbColumn
	{
		public CustomDbColumn(IDataColumn col, int ordinal)
		{
			this.ColumnName = col.Name;
			this.ColumnOrdinal = ordinal;
			this.AllowDBNull = col.AllowNull;
			this.DataType = col.ColumnType;
			this.DataTypeName = this.DataType.Name;
		}
	}

	public override object this[int ordinal] => GetValue(ordinal);

	public override object this[string name] => GetValue(this.GetOrdinal(name));

	public override int Depth => 0;

	public override int FieldCount => this.columns.Length;

	public override bool HasRows => dr.HasRows;

	public override bool IsClosed => dr.IsClosed;

	public override int RecordsAffected => 0;

	public override bool GetBoolean(int ordinal)
	{
		return GetColumn(ordinal).GetValue<bool>(this);
	}

	public override byte GetByte(int ordinal)
	{
		return GetColumn(ordinal).GetValue<byte>(this);
	}

	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		//if (buffer == null) throw new ArgumentNullException(nameof(buffer));
		return GetColumn(ordinal).GetData<byte>(this, buffer, dataOffset, bufferOffset, length);
	}

	public override char GetChar(int ordinal)
	{
		return GetColumn(ordinal).GetValue<char>(this);
	}

	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
	{
		if (buffer == null) throw new ArgumentNullException(nameof(buffer));
		return GetColumn(ordinal).GetData<char>(this, buffer, dataOffset, bufferOffset, length);
	}

	public override string GetDataTypeName(int ordinal)
	{
		return GetColumn(ordinal).ColumnType.Name;
	}

	public override DateTime GetDateTime(int ordinal)
	{
		return GetColumn(ordinal).GetValue<DateTime>(this);
	}

	public override decimal GetDecimal(int ordinal)
	{
		return GetColumn(ordinal).GetValue<decimal>(this);
	}

	public override double GetDouble(int ordinal)
	{
		return GetColumn(ordinal).GetValue<double>(this);
	}

	public override IEnumerator GetEnumerator()
	{
		throw new NotSupportedException();
	}

	public override Type GetFieldType(int ordinal)
	{
		return GetColumn(ordinal).ColumnType;
	}

	public override float GetFloat(int ordinal)
	{
		return GetColumn(ordinal).GetValue<float>(this);
	}

	public override Guid GetGuid(int ordinal)
	{
		return GetColumn(ordinal).GetValue<Guid>(this);
	}

	public override short GetInt16(int ordinal)
	{
		return GetColumn(ordinal).GetValue<short>(this);
	}

	public override int GetInt32(int ordinal)
	{
		return GetColumn(ordinal).GetValue<int>(this);
	}

	public override long GetInt64(int ordinal)
	{
		return GetColumn(ordinal).GetValue<long>(this);
	}

	public override string GetName(int ordinal)
	{
		return GetColumn(ordinal).Name;
	}

	public override int GetOrdinal(string name)
	{
		for (int i = 0; i < this.columns.Length; i++)
		{
			if (name == this.columns[i].Name)
				return i;
		}
		return -1;
	}

	public override string GetString(int ordinal)
	{
		return GetColumn(ordinal).GetValue<string>(this);
	}

	public override object GetValue(int ordinal)
	{
		if (this.IsDBNull(ordinal))
		{
			return DBNull.Value;
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
		return this.columns[ordinal].IsDbNull(this);
	}

	IDataColumn GetColumn(int ordinal)
	{
		return this.columns[ordinal];
	}

	public override T GetFieldValue<T>(int ordinal)
	{
		return GetColumn(ordinal).GetValue<T>(this);
	}

	public override bool NextResult()
	{
		return dr.NextResult();
	}

	public override bool Read()
	{
		return dr.Read();
	}

	public override DataTable? GetSchemaTable()
	{
		return SchemaTable.GetSchemaTable(this.GetColumnSchema());
	}

	public ReadOnlyCollection<DbColumn> GetColumnSchema()
	{
		return schema;
	}

	public override void Close()
	{
		this.dr.Close();
	}

#if ASYNC
	public override Task CloseAsync()
	{
		return dr.CloseAsync();
	}
#endif

	protected override void Dispose(bool disposing)
	{
		if (disposing)
		{
			dr.Dispose();
		}
	}
}
