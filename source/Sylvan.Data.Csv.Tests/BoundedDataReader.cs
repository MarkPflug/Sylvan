using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv;

class BoundedDataReader : DataReaderAdapter
{
	readonly int rows;
	int count = 0;

	public BoundedDataReader(DbDataReader dr, int rows) : base(dr)
	{
		this.rows = rows;
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		throw new NotImplementedException();
	}

	public override bool Read()
	{
		var success = count < rows && base.Read();

		if (success)
		{
			count++;
		}
		return success;
	}
}
