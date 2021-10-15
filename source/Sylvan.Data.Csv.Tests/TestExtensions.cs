using System.Data.Common;

namespace Sylvan.Data.Csv
{
	static class TestExtensions
	{
		public static byte[] GetBytes(this DbDataReader dr, int ordinal)
		{
			var len = (int) dr.GetBytes(ordinal, 0, null, 0, 0);
			var buffer = new byte[len];
			dr.GetBytes(ordinal, 0, buffer, 0, len);
			return buffer;
		}
	}
}
