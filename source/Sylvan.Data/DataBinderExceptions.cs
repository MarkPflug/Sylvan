using System;
using System.Collections.Generic;

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

	public sealed class DataBinderException : Exception
	{
		public IReadOnlyList<string> UnboundProperties { get;}
		public IReadOnlyList<string> UnboundColumns { get; }

		internal DataBinderException(string[]? unboundProperties, string[]? unboundColumns)
		{
			this.UnboundProperties = unboundProperties ?? Array.Empty<string>();
			this.UnboundColumns = unboundColumns ?? Array.Empty<string>();
		}
	}
}
