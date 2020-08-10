using System;

#if DEDUPE_STRINGS

namespace Sylvan.Data.Csv
{
	/// <summary>
	/// 
	/// </summary>
	public interface IStringPool
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public string? GetString(ReadOnlySpan<char> str);
	}
}

namespace Sylvan
{
	/// <summary>
	/// 
	/// </summary>
	public partial class StringPool : Sylvan.Data.Csv.IStringPool
	{

	}

	/// <summary>
	/// 
	/// </summary>
	public partial class StringPoolFast : Sylvan.Data.Csv.IStringPool
	{

	}
}

#endif
