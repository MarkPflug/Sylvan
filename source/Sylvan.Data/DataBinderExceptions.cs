using System;

namespace Sylvan.Data
{

	public sealed class InvalidEnumValueDataBinderException : FormatException
	{
		public string Value { get; }
		public Type EnumType { get; }

		internal InvalidEnumValueDataBinderException(Type enumType, string value)
		{
			this.EnumType = enumType;
			this.Value = value;
		}
	}
}
