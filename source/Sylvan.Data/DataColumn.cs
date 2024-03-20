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
