using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Sylvan.Data
{
	/// <summary>
	/// Extension methods for DbDataReader.
	/// </summary>
	public static class DataExtensions
	{
		/// <summary>
		/// Converts an IDataReader to a DbDataReader.
		/// This conversion might be a no-op if the IDataReader is already
		/// a DbDataReader, or it might adapt via a wrapper.
		/// </summary>
		public static DbDataReader AsDbDataReader(this IDataReader r)
		{
			if (r is DbDataReader dr) return dr;
			return new IDataReaderAdpater(r);
		}

		/// <summary>
		/// Binds the DbDataReader data to produce a sequence of T.
		/// </summary>
		/// <typeparam name="T">The type of record to bind to.</typeparam>
		/// <param name="reader">The data reader.</param>
		public static IEnumerable<T> GetRecords<T>(this DbDataReader reader) where T : new()
		{
			var binder = DataBinder.Create<T>(reader);
			while (reader.Read())
			{
				var item = new T();
				binder.Bind(reader, item);
				yield return item;
			}
		}

		/// <example>
		/// var reader = seq.AsDataReader()
		/// </example>
		public static DbDataReader AsDataReader<T>(this IEnumerable<T> seq) where T : class
		{
			return new ObjectDataReader<T>(seq);
		}

		/// <summary>
		/// Selects a subset of columns for a DbDataReader.
		/// </summary>
		/// <param name="reader">The DbDataReader to select columns from.</param>
		/// <param name="ordinalsSelector">A function to select the column ordinals.</param>
		/// <returns>Returns a new DbDataReader containing just the selected columns.</returns>
		public static DbDataReader Select(this DbDataReader reader, Func<DbDataReader, int[]> ordinalsSelector)
		{
			var o = ordinalsSelector(reader);
			return new TransformDataReader(reader, o);
		}

		/// <summary>
		/// Selects a subset of columns for a DbDataReader.
		/// </summary>
		/// <param name="reader">The DbDataReader to select columns from.</param>
		/// <param name="ordinals">A column ordinals to select.</param>
		/// <returns>Returns a new DbDataReader containing just the selected columns.</returns>
		public static DbDataReader Select(this DbDataReader reader, params int[] ordinals)
		{
			return new TransformDataReader(reader, ordinals);
		}

		/// <summary>
		/// Selects a subset of columns for a DbDataReader.
		/// </summary>
		/// <param name="reader">The DbDataReader to select columns from.</param>
		/// <param name="columnNames">The names of the columns to select.</param>
		/// <returns>Returns a new DbDataReader containing just the selected columns.</returns>
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

		/// <summary>
		/// Applies a filter predicate to the rows of a DbDataReader.
		/// </summary>
		/// <param name="reader">A DbDataReader.</param>
		/// <param name="predicate">A filter predicate to filter the rows.</param>
		/// <returns>A new DbDataReader that produces the filtered rows.</returns>
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
