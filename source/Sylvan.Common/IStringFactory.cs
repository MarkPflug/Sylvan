namespace Sylvan
{
	/// <summary>
	/// An interface that allows de-duplication of strings upon construction.
	/// </summary>
	/// <remarks>
	/// This is intended to allow de-duplication of strings when 
	/// reading from buffers, such as files, databases, json, serialized input, etc.
	/// </remarks>
	public interface IStringFactory
	{
		/// <summary>
		/// Gets a string that contains the contents of the buffer.
		/// </summary>
		string GetString(char[] buffer, int offset, int length);
	}
}
