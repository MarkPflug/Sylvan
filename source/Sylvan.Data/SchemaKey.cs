using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Sylvan.Data;

sealed class SchemaKey : IEquatable<SchemaKey>
{
	readonly ColumnInfo[] columns;
	readonly int hash;

	public SchemaKey(IReadOnlyList<DbColumn> schema)
	{
		var count = schema.Count;
		this.columns = new ColumnInfo[count];
		int hash = 0;
		for (int i = 0; i < count; i++)
		{
			this.columns[i] = new ColumnInfo(schema[i]);
			hash = hash * 31 + this.columns[i].GetHashCode();
		}
		this.hash = hash;
	}

	public override int GetHashCode()
	{
		return hash;
	}

	public bool Equals(SchemaKey other)
	{
		if (!(this.hash == other.hash &&
			this.columns.Length == other.columns.Length))
		{
			return false;
		}
		for (int i = 0; i < columns.Length; i++)
		{
			if (!this.columns[i].Equals(other.columns[i]))
				return false;
		}
		return true;
	}

	readonly struct ColumnInfo
	{
		readonly string name;
		readonly Type type;
		readonly bool allowDbNull;

		public ColumnInfo(DbColumn col)
		{
			this.name = col.ColumnName;
			this.type = col.DataType;
			this.allowDbNull = col.AllowDBNull ?? true;
		}

		public override int GetHashCode()
		{
			return
				(name.GetHashCode() * 31
				+ type.GetHashCode()) * 31 +
				allowDbNull.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			return obj is ColumnInfo ci && this.Equals(ci);
		}

		public bool Equals(ColumnInfo ci)
		{
			return
				this.name == ci.name &&
				this.type == ci.type &&
				this.allowDbNull == ci.allowDbNull;
		}
	}
}
