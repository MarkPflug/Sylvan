using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data;

class ProgressDataReader : DataReaderAdapter
{
	readonly Action<int> progressCallback;

	int row = 0;

	public ProgressDataReader(DbDataReader dr, Action<int> progressCallback) : base(dr)
	{
		this.progressCallback = progressCallback;
	}

	public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
		progressCallback(row++);
		return base.ReadAsync(cancellationToken);
	}

	public override bool Read()
	{
		progressCallback(row++);
		return base.Read();
	}
}
