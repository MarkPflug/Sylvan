using System.Data.Common;

namespace Sylvan.Data.Csv;


/// <summary>
/// A base implementation of ICsvSchemaProvider.
/// </summary>
public abstract class CsvSchemaProvider : ICsvSchemaProvider
{
	/// <inheritdoc/>
	public abstract DbColumn? GetColumn(string? name, int ordinal);

	/// <inheritdoc/>
	public int GetFieldCount(CsvDataReader reader)
	{
		return reader.RowFieldCount;
	}
}

/// <summary>
/// Provides the ability to specify a schema for a CsvDataReader.
/// </summary>
public interface ICsvSchemaProvider
{
	/// <summary>
	/// Gets the schema for a column with the given name and/or ordinal.
	/// </summary>
	/// <param name="name">The name of the column, or null if the data contains no headers.</param>
	/// <param name="ordinal">The zero-based ordinal of the column.</param>
	/// <returns>A DbColumn for the given column, or null to use the default.</returns>
	DbColumn? GetColumn(string? name, int ordinal);

	/// <summary>
	/// Gets the number of fields in the schema.
	/// </summary>
	/// <param name="reader">The data reader being initialized.</param>
	/// <returns>The number of fields.</returns>
	int GetFieldCount(CsvDataReader reader)
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_1_OR_GREATER
	{
		return reader.RowFieldCount;
	}
#else
		; // abstract
#endif
}
