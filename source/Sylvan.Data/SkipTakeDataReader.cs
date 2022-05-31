using System.Data.Common;

namespace Sylvan.Data;

sealed class SkipTakeDataReader : DataReaderAdapter
{
	long skip;
	long take;

	public SkipTakeDataReader(DbDataReader dr, long skip, long take) : base(dr)
	{
		this.skip = skip;
		this.take = take;
	}

	public override bool Read()
	{
		while (skip > 0)
		{
			skip -= 1;
			if (!Reader.Read())
			{
				return false;
			}
		}
		switch(take)
		{
			case 0:
				break;
			default:
				take -= 1;
				goto case -1;
			case -1:
				return Reader.Read();
		}
		return false;
	}
}
