using System;


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
		string? GetString(char[] buffer, int offset, int length);
	}
}

//namespace Sylvan
//{
//	/// <summary>
//	/// 
//	/// </summary>
//	public partial class StringPool : Sylvan.Data.Csv.IStringPool
//	{

//	}

//	/// <summary>
//	/// 
//	/// </summary>
//	public partial class StringPoolFast : Sylvan.Data.Csv.IStringPool
//	{

//	}
//}

