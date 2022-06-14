using System;

namespace Sylvan.Data;

/// <summary>
/// Specifies how columns are bound to properties.
/// </summary>
[Flags]
public enum DataBindingMode
{
	/// <summary>
	/// Allows any combination of unbound properties and columns.
	/// </summary>
	Any = 0,
	/// <summary>
	/// Requires all properties be bound to a column.
	/// </summary>
	AllProperties = 1,
	/// <summary>
	/// Requires all columns be bound to a property.
	/// </summary>
	AllColumns = 2,
	/// <summary>
	/// Requires that all columns and all properties be bound.
	/// </summary>
	All = 3,
}
