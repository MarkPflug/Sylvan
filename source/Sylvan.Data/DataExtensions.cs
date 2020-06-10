using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	public static class DataExtensions
	{
		public static DbDataReader AsDbDataReader(this IDataReader r)
		{
			if (r is DbDataReader dr) return dr;
			return new IDataReaderAdpater(r);
		}

		/// <example>
		/// var reader = seq.CreateDataReader()
		/// </example>
		public static DbDataReader AsDataReader<T>(this IEnumerable<T> seq)
		{
			return new ObjectDataReader<T>(seq.GetEnumerator());
		}

		//public static DbDataReader Transform(this DbDataReader reader, Action<TransformBuilder> transform)
		//{
		//	return new TransformDataReader(reader, transform);
		//}
	}
}
