using System;
using System.Data.Common;

namespace Sylvan.Data;

sealed class TakeWhileDataReader : DataReaderAdapter
{
	readonly Func<DbDataReader, bool> predicate;
	int state;

	public TakeWhileDataReader(DbDataReader dr, Func<DbDataReader, bool> predicate) : base(dr)
	{
		this.predicate = predicate;
		this.state = 0;
	}

	public override bool Read()
	{
		if (state == 0)
		{
			if (!Reader.Read())
			{
				this.state = 1;
				return false;
			}
			if (!predicate(this))
			{
				this.state = 2;
				return false;
			}
			return true;
		}
		return false;
	}
}
