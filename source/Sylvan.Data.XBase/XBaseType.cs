namespace Sylvan.Data.XBase
{
	enum XBaseType
		: byte
	{
		Character = (byte)'C',
		Numeric = (byte)'N',
		Logical = (byte)'L',
		Date = (byte)'D',
		Memo = (byte)'M',
		Float = (byte)'F',
		General = (byte)'G',
		Currency = (byte)'Y',
		DateTime = (byte)'T',
		Integer = (byte)'I',
		Variant = (byte)'X',
		Double = (byte)'B',
		NullFlags = (byte)'0',
		VarBinary = (byte)'Q',
		VarChar = (byte)'V',
		Blob = (byte)'W',
	}
}
