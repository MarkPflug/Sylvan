using System.Data.Common;

namespace Sylvan.Data
{
	public sealed class TruncatingDataReader : DataReaderAdapter
	{
		readonly int len;

		public TruncatingDataReader(DbDataReader dr, int len) : base(dr)
		{
			this.len = len;
		}

		public override string? GetString(int ordinal)
		{
			var str = dr.GetString(ordinal);
			if (str == null || str.Length == 0) return null;
			return
				str.Length > len
				? str.Substring(0, len)
				: str;
		}
	}
}
