using System;
using System.Data.Common;

namespace Sylvan.Data;

class ProgressDataReader : DataReaderAdapter
{
	readonly Action<int> progressCallback;

	int row = 0;

	public ProgressDataReader(DbDataReader dr, Action<int> progressCallback) : base(dr)
	{
		this.progressCallback = progressCallback;
	}

	public override bool Read()
	{
		progressCallback(row++);
		return base.Read();
	}
}
