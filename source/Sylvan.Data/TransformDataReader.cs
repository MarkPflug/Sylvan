using System;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data;

sealed class TransformDataReader : DbDataReader, IDbColumnSchemaGenerator
{
	static readonly Func<DbDataReader, bool> TruePredicate = DbDataReader => true;

	readonly DbDataReader reader;

	readonly int[] columnMap;
	readonly Func<DbDataReader, bool> predicate;

	int row = -1;
	int underlyingRow = -1;

	// build on-demand
	ReadOnlyCollection<DbColumn>? schemaCols;

	public TransformDataReader(DbDataReader dataReader, int[]? ordinals = null, Func<DbDataReader, bool>? predicate = null)
	{
		this.reader = dataReader;

		if (ordinals != null)
		{
			foreach (var ordinal in ordinals)
			{
				if (ordinal < 0 || ordinal > reader.FieldCount)
				{
					throw new ArgumentOutOfRangeException(nameof(ordinal));
				}
			}
			this.columnMap = (int[])ordinals.Clone();
		}
		else
		{
			this.columnMap = new int[reader.FieldCount];
			for (int i = 0; i < columnMap.Length; i++)
			{
				columnMap[i] = i;
			}
		}

		this.predicate = predicate ?? TruePredicate;
	}

	public override object this[int ordinal] => this.GetValue(ordinal);

	public override object this[string name] => this.GetValue(GetOrdinal(name));

	public override int Depth => 0;

	public override int FieldCount => columnMap.Length;

	public override int VisibleFieldCount => FieldCount;

	public override bool HasRows => reader.HasRows;

	public override bool IsClosed => reader.IsClosed;

	public override int RecordsAffected => reader.RecordsAffected;

	int Map(int ordinal)
	{
		if ((uint)ordinal > (uint)columnMap.Length)
			throw new ArgumentOutOfRangeException(nameof(ordinal));

		return columnMap[ordinal];
	}

	public override bool GetBoolean(int ordinal)
	{
		return reader.GetBoolean(Map(ordinal));
	}

	public override byte GetByte(int ordinal)
	{
		return reader.GetByte(Map(ordinal));
	}

	public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
	{
		return reader.GetBytes(Map(ordinal), dataOffset, buffer, bufferOffset, length);
	}

	public override char GetChar(int ordinal)
	{
		return reader.GetChar(Map(ordinal));
	}

	public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
	{
		return reader.GetChars(Map(ordinal), dataOffset, buffer, bufferOffset, length);
	}

	public override string GetDataTypeName(int ordinal)
	{
		return reader.GetDataTypeName(Map(ordinal));
	}

	public override DateTime GetDateTime(int ordinal)
	{
		return reader.GetDateTime(Map(ordinal));
	}

	public override decimal GetDecimal(int ordinal)
	{
		return reader.GetDecimal(Map(ordinal));
	}

	public override double GetDouble(int ordinal)
	{
		return reader.GetDouble(Map(ordinal));
	}

	public override IEnumerator GetEnumerator()
	{
		return new DbEnumerator(this);
	}

	public override Type GetFieldType(int ordinal)
	{
		return reader.GetFieldType(Map(ordinal));
	}

	public override float GetFloat(int ordinal)
	{
		return reader.GetFloat(Map(ordinal));
	}

	public override Guid GetGuid(int ordinal)
	{
		return reader.GetGuid(Map(ordinal));
	}

	public override short GetInt16(int ordinal)
	{
		return reader.GetInt16(Map(ordinal));
	}

	public override int GetInt32(int ordinal)
	{
		return reader.GetInt32(Map(ordinal));
	}

	public override long GetInt64(int ordinal)
	{
		return reader.GetInt64(Map(ordinal));
	}

	public override string GetName(int ordinal)
	{
		return reader.GetName(Map(ordinal));
	}

	public override int GetOrdinal(string name)
	{
		for (int i = 0; i < this.columnMap.Length; i++)
		{
			if (name == reader.GetName(Map(i)))
			{
				return i;
			}
		}
		throw new IndexOutOfRangeException();
	}

	public override string GetString(int ordinal)
	{
		return reader.GetString(Map(ordinal));
	}

	public override T GetFieldValue<T>(int ordinal)
	{
		return reader.GetFieldValue<T>(Map(ordinal));
	}

	public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancel)
	{
		return reader.GetFieldValueAsync<T>(Map(ordinal), cancel);
	}

	public override Type GetProviderSpecificFieldType(int ordinal)
	{
		return reader.GetProviderSpecificFieldType(Map(ordinal));
	}

	public override object GetProviderSpecificValue(int ordinal)
	{
		return reader.GetProviderSpecificValue(Map(ordinal));
	}

	public override int GetProviderSpecificValues(object[] values)
	{
		if (values is null)
			throw new ArgumentNullException(nameof(values));

		var c = Math.Min(values.Length, this.columnMap.Length);
		for (int i = 0; i < c; i++)
		{
			values[i] = GetProviderSpecificValue(i);
		}
		return c;
	}

	public override Stream GetStream(int ordinal)
	{
		return reader.GetStream(Map(ordinal));
	}

	public override TextReader GetTextReader(int ordinal)
	{
		return reader.GetTextReader(Map(ordinal));
	}

	public override object GetValue(int ordinal)
	{
		return reader.GetValue(Map(ordinal));
	}

	public override int GetValues(object[] values)
	{
		if (values is null)
			throw new ArgumentNullException(nameof(values));

		var c = Math.Min(values.Length, this.columnMap.Length);
		for (int i = 0; i < c; i++)
		{
			values[i] = GetValue(i);
		}
		return c;
	}

	public override bool IsDBNull(int ordinal)
	{
		return reader.IsDBNull(Map(ordinal));
	}

	public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancel)
	{
		return reader.IsDBNullAsync(Map(ordinal), cancel);
	}

	protected override void Dispose(bool disposing)
	{
		if (disposing)
		{
			reader.Dispose();
		}
	}

	public override bool NextResult()
	{
		// I don't think this makes sense to call the
		// NextResult on the base here.
		// For DataReaders that support it (sqlclient)
		// the shape of the next result set would need
		// a new transformation.
		return false;
	}

	public override Task<bool> NextResultAsync(CancellationToken cancel)
	{
		return Task.FromResult(false);
	}

	public override async Task<bool> ReadAsync(CancellationToken cancel)
	{
		while (await this.reader.ReadAsync(cancel).ConfigureAwait(false))
		{
			underlyingRow++;
			if (this.predicate(this))
			{
				row++;
				return true;
			}
		}
		return false;
	}

	public override bool Read()
	{
		while (this.reader.Read())
		{
			underlyingRow++;
			if (this.predicate(this))
			{
				row++;
				return true;
			}
		}
		return false;
	}

	public ReadOnlyCollection<DbColumn> GetColumnSchema()
	{
		var innerSchema = reader.GetColumnSchema();
		if (this.schemaCols == null)
		{
			var sb = new Schema.Builder();
			for (int i = 0; i < columnMap.Length; i++)
			{
				sb.Add(new Schema.Column.Builder(innerSchema[columnMap[i]]));
			}
			this.schemaCols = sb.Build();

		}
		return this.schemaCols;
	}

	public override DataTable GetSchemaTable()
	{
		return SchemaTable.GetSchemaTable(GetColumnSchema());
	}

	public override void Close()
	{
		reader.Close();
	}
}
