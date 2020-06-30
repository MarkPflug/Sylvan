using System.Data.Common;

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// Provides the ability to specify a schema for a CsvDataReader.
	/// </summary>
	public interface ICsvSchemaProvider
	{
		/// <summary>
		/// Gets the schema for a column with the given name and/or oridinal.
		/// </summary>
		/// <param name="name">The name of the column, or null if the data contains no headers.</param>
		/// <param name="ordinal">The zero-based ordinal of the column.</param>
		/// <returns>A DbColumn for the given column, or null to use the default.</returns>
		DbColumn? GetColumn(string? name, int ordinal);
	}
}
