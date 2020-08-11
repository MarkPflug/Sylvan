namespace Sylvan.Data.Csv
{
	/// <summary>
	/// An interface that allows <see cref="CsvDataReader"/> to de-duplicate strings
	/// as they are read from the CSV text.
	/// </summary>
	/// <remarks>
	/// Tabular data files often contain a lot of duplicate string values. This interface is
	/// intended to allow de-duplicating such strings while avoiding creating a lot of garbage.
	/// </remarks>
	public interface IStringPool
	{
		/// <summary>
		/// Gets a string value from the pool containing the characters from <paramref name="buffer"/>.
		/// </summary>
		/// <returns>
		/// A string, or null. A null value might indicate the string didn't meet the criteria of the pool. 
		/// In which case it becomes the policy of the caller to construct the string.
		/// </returns>
		string? GetString(char[] buffer, int offset, int length);
	}
}
