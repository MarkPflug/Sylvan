using System;

namespace Sylvan
{
	/// <summary>
	/// Allows de-duplication of strings.
	/// </summary>
	/// <remarks>
	/// This is intended to allow de-duplication of strings when
	/// reading from external buffers, such as databases, json, serialized input, etc.
	/// </remarks>
	public interface IStringPool
	{
		/// <summary>
		/// Gets or adds a string that is equal to the given span.
		/// </summary>
		/// <param name="str">The sequence of characters.</param>
		/// <returns>A string, or null.</returns>
		string? GetString(ReadOnlySpan<char> str);
	}

	/// <summary>
	/// An IStringPool implementation.
	/// </summary>
	public partial class StringPool : IStringPool
	{
	}
}
