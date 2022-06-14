using System;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;

namespace Sylvan.Data;

/// <summary>
/// A base class for DbDataReaders that wrap and modify other DbDataReaders.
/// </summary>
public abstract partial class DataReaderAdapter : DbDataReader, IDbColumnSchemaGenerator
{
	readonly DbDataReader dr;

	/// <summary>
	/// Gets the inner data reader.
	/// </summary>
	protected DbDataReader Reader => dr;

	/// <summary>
	/// Creates a new DataReaderAdapter.
	/// </summary>
	public DataReaderAdapter(DbDataReader dr)
	{
		this.dr = dr;
	}

	/// <inheritdoc/>
	public override object this[int ordinal] => dr[ordinal];

	/// <inheritdoc/>
	public override object this[string name] => dr[name];

	/// <inheritdoc/>
	public override int Depth => dr.Depth;

	/// <inheritdoc/>
	public override int FieldCount => dr.FieldCount;

	/// <inheritdoc/>
	public override bool HasRows => dr.HasRows;

	/// <inheritdoc/>
	public override bool IsClosed => dr.IsClosed;

	/// <inheritdoc/>
	public override int RecordsAffected => dr.RecordsAffected;

	/// <inheritdoc/>
	public override bool GetBoolean(int ordinal)
	{
		return dr.GetBoolean(ordinal);
	}

	/// <inheritdoc/>
	public override byte GetByte(int ordinal)
	{
		return dr.GetByte(ordinal);
	}

	/// <inheritdoc/>
	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		if (buffer == null) throw new ArgumentNullException(nameof(buffer));

		return dr.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
	}

	/// <inheritdoc/>
	public override char GetChar(int ordinal)
	{
		return dr.GetChar(ordinal);
	}

	/// <inheritdoc/>
	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
	{
		if (buffer == null) throw new ArgumentNullException(nameof(buffer));

		return dr.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
	}

	/// <inheritdoc/>
	public override string GetDataTypeName(int ordinal)
	{
		return dr.GetDataTypeName(ordinal);
	}

	/// <inheritdoc/>
	public override DateTime GetDateTime(int ordinal)
	{
		return dr.GetDateTime(ordinal);
	}

	/// <inheritdoc/>
	public override decimal GetDecimal(int ordinal)
	{
		return dr.GetDecimal(ordinal);
	}

	/// <inheritdoc/>
	public override double GetDouble(int ordinal)
	{
		return dr.GetDouble(ordinal);
	}

	/// <inheritdoc/>
	public override IEnumerator GetEnumerator()
	{
		while (this.Read())
		{
			yield return this;
		}
	}

	/// <inheritdoc/>
	public override Type GetFieldType(int ordinal)
	{
		return dr.GetFieldType(ordinal);
	}

	/// <inheritdoc/>
	public override float GetFloat(int ordinal)
	{
		return dr.GetFloat(ordinal);
	}

	/// <inheritdoc/>
	public override Guid GetGuid(int ordinal)
	{
		return dr.GetGuid(ordinal);
	}

	/// <inheritdoc/>
	public override short GetInt16(int ordinal)
	{
		return dr.GetInt16(ordinal);
	}

	/// <inheritdoc/>
	public override int GetInt32(int ordinal)
	{
		return dr.GetInt32(ordinal);
	}

	/// <inheritdoc/>
	public override long GetInt64(int ordinal)
	{
		return dr.GetInt64(ordinal);
	}

	/// <inheritdoc/>
	public override string GetName(int ordinal)
	{
		return dr.GetName(ordinal);
	}

	/// <inheritdoc/>
	public override int GetOrdinal(string name)
	{
		return dr.GetOrdinal(name);
	}

	/// <inheritdoc/>
	public override string GetString(int ordinal)
	{
		return dr.GetString(ordinal);
	}

	/// <inheritdoc/>
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

	/// <inheritdoc/>
	public override Stream GetStream(int ordinal)
	{
		return new DbDataReaderStream(this, ordinal);
	}

	/// <inheritdoc/>
	public override TextReader GetTextReader(int ordinal)
	{
		return new DbDataReaderTextReader(this, ordinal);
	}

	/// <inheritdoc/>
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

	/// <inheritdoc/>
	public override bool IsDBNull(int ordinal)
	{
		return dr.IsDBNull(ordinal);
	}

	/// <inheritdoc/>
	public override T GetFieldValue<T>(int ordinal)
	{
		return dr.GetFieldValue<T>(ordinal);
	}

	/// <inheritdoc/>
	public override bool NextResult()
	{
		return dr.NextResult();
	}

	/// <inheritdoc/>
	public override bool Read()
	{
		return dr.Read();
	}

	/// <inheritdoc/>
	public override DataTable? GetSchemaTable()
	{
		return dr.GetSchemaTable();
	}

	/// <inheritdoc/>
	public ReadOnlyCollection<DbColumn> GetColumnSchema()
	{
		return dr.GetColumnSchema();
	}
}
