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
			return new ObjectDataReader<T>(seq);
		}

		public static DbDataReader Select(this DbDataReader reader, params int[] ordinals)
		{
			return new TransformDataReader(reader, ordinals);
		}

		public static DbDataReader Select(this DbDataReader reader, params string[] columnNames)
		{
			var ordinals = GetOrdinals(reader, columnNames);
			return new TransformDataReader(reader, ordinals);
		}

		static int[] GetOrdinals(DbDataReader reader, string[] names)
		{
			int[] ordinals = new int[names.Length];

			for (int i = 0; i < names.Length; i++)
			{
				var idx = reader.GetOrdinal(names[i]);
				if (idx < 0)
					throw new ArgumentOutOfRangeException(nameof(names));
				ordinals[i] = idx;
			}
			return ordinals;
		}

		public static DbDataReader Where(this DbDataReader reader, Func<DbDataReader, bool> predicate)
		{
			if (reader == null) throw new ArgumentNullException(nameof(reader));
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));
			// TODO: TransformDataReader needs to merge into a new object rather than
			// nest. nesting will lead to excessive method call overhead.
			return new TransformDataReader(reader, null, predicate);
		}

		//public static DbDataReader Take(this DbDataReader reader, int count)
		//{
		//	throw new NotImplementedException();
		//}

		//public static DbDataReader Skip(this DbDataReader reader, int count)
		//{
		//	throw new NotImplementedException();
		//}
	}
}
