using System;
using System.Collections.Generic;

namespace Sylvan.Data
{
	/// <summary>
	/// An exception thrown when a data binder encounters an invalid enum value.
	/// </summary>
	public sealed class InvalidEnumValueDataBinderException : FormatException
	{
		/// <summary>
		/// The invalid string value.
		/// </summary>
		public string Value { get; }

		/// <summary>
		/// The type of enum being bound.
		/// </summary>
		public Type EnumType { get; }

		internal InvalidEnumValueDataBinderException(Type enumType, string value)
		{
			this.EnumType = enumType;
			this.Value = value;
		}
	}

	/// <summary>
	/// A base type for exceptions thrown when binding data.
	/// </summary>
	public abstract class DataBinderException : Exception { }

	/// <summary>
	/// An exception thrown when a data binder enounters unbound columns or properties and is configured
	/// to require them to be bound.
	/// </summary>
	public sealed class UnboundMemberException : DataBinderException
	{
		/// <summary>
		/// The names of the unbound properties.
		/// </summary>
		public IReadOnlyList<string> UnboundProperties { get;}

		/// <summary>
		/// The names of the unbound columns.
		/// </summary>
		public IReadOnlyList<string> UnboundColumns { get; }

		internal UnboundMemberException(string[]? unboundProperties, string[]? unboundColumns)
		{
			this.UnboundProperties = unboundProperties ?? Array.Empty<string>();
			this.UnboundColumns = unboundColumns ?? Array.Empty<string>();
		}
	}
}
