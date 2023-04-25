using System;
using System.Collections.Generic;

namespace Sylvan.Data;

/// <summary>
/// An exception thrown when a data binder encounters an invalid enum value.
/// </summary>
public sealed class InvalidEnumValueException : FormatException
{
	/// <summary>
	/// The invalid string value.
	/// </summary>
	public string Value { get; }

	/// <summary>
	/// The type of enum being bound.
	/// </summary>
	public Type EnumType { get; }

	internal InvalidEnumValueException(Type enumType, string value)
	{
		this.EnumType = enumType;
		this.Value = value;
	}
}

/// <summary>
/// A base type for exceptions thrown when binding data.
/// </summary>
public class DataBinderException : Exception
{
	const string DefaultMessage = "DataBinder encountered an exception.";

	/// <summary>
	/// Gets the ordinal of the column that encountered an exception.
	/// </summary>
	public int Ordinal
	{
		get;
	}

	internal DataBinderException()
		: base(DefaultMessage)
	{
		this.Ordinal = -1;
	}

	internal DataBinderException(int ordinal, string message)
		: base(message)
	{
		this.Ordinal = ordinal;
	}

	internal DataBinderException(int ordinal, Exception exception)
		: base(DefaultMessage, exception)
	{
		this.Ordinal = ordinal;
	}

	internal DataBinderException(int ordinal, string message, Exception exception)
		: base(message, exception)
	{
		this.Ordinal = ordinal;
	}
}

/// <summary>
/// An exception thrown when a data binder encounters unbound columns or properties and is configured
/// to require them to be bound.
/// </summary>
public sealed class UnboundMemberException : DataBinderException
{
	const string UnboundMemberMessage = "The data could not be bound to the target type, it either contains too many or to few columns. Specify a different BindingMode to allow binding a subset of columns or properties.";

	/// <summary>
	/// The names of the unbound properties.
	/// </summary>
	public IReadOnlyList<string> UnboundProperties { get; }

	/// <summary>
	/// The names of the unbound columns.
	/// </summary>
	public IReadOnlyList<string> UnboundColumns { get; }

	internal UnboundMemberException(string[]? unboundProperties, string[]? unboundColumns)
		: base(-1, UnboundMemberMessage)
	{
		this.UnboundProperties = unboundProperties ?? Array.Empty<string>();
		this.UnboundColumns = unboundColumns ?? Array.Empty<string>();
	}
}
