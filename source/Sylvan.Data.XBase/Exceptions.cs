using System;

namespace Sylvan.Data.XBase
{
	public sealed class EncodingNotSupportedException : NotSupportedException
	{
		public int Code { get; }

		public EncodingNotSupportedException(int code)
		{
			this.Code = code;
		}
	}
}
