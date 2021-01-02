using System.Text;

namespace Sylvan.Data.XBase
{
	public sealed class XBaseDataReaderOptions
	{
		internal static readonly XBaseDataReaderOptions Default =
			new XBaseDataReaderOptions()
			{
				IgnoreMemo = false,
				Encoding = null,
			};
		
		/// <summary>
		/// Ignore a missing memo stream.
		/// </summary>
		/// <remarks>
		/// When false, the default, the DBaseDataReader will throw an exception
		/// during initialization if a memo stream is expected but not provided.
		/// When true, any data access that requires the memo stream will produce an exception
		/// upon acccess. Non-memo fields are all available.
		/// </remarks>
		internal bool IgnoreMemo { get; set; }

		/// <summary>
		/// Specifies an explicit encoding.
		/// </summary>
		/// <remarks>
		/// By default, the encoding specified in the file header will be used.
		/// </remarks>
		public Encoding? Encoding { get; set; }
    }
}
