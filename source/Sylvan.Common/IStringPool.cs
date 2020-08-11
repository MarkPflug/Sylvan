namespace Sylvan
{
	/// <summary>
	/// Allows de-duplication of strings.
	/// </summary>
	/// <remarks>
	/// This is intended to allow de-duplication of strings when 
	/// reading from buffers, such as files, databases, json, serialized input, etc.
	/// </remarks>
	public interface IStringPool
	{
		/// <summary>
		/// Gets or adds a string that is equal to the given span.
		/// </summary>
		string? GetString(char[] buffer, int offset, int length);
	}

	/// <summary>
	/// An IStringPool implementation.
	/// </summary>
	public partial class StringPool : IStringPool
	{
	}

	/// <summary>
	/// An IStringPool implementation.
	/// </summary>
	public partial class StringPool : IStringPool
	{
	}
}
