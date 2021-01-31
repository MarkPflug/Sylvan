using System;

namespace Sylvan.Data
{
	/// <summary>
	/// Specifies how columns are bound to properties.
	/// </summary>
	[Flags]
	public enum DataBindMode
	{
		/// <summary>
		/// Requires neither all column nor all properties be bound.
		/// </summary>
		Neither = 0,
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
		Both = 3,
	}
}
