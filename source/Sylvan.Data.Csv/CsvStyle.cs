using System;

namespace Sylvan.Data.Csv;

/// <summary>
/// The CSV quoting and escaping styles.
/// </summary>
public enum CsvStyle
{
	/// <summary>
	/// Parses using the standard RFC4180 mode.
	/// </summary>
	Standard = 1,

	/// <summary>
	/// Interprets fields as if they are implicitly quoted. Delimiters and new lines within fields are preceded by an escape character.
	/// </summary>
	[Obsolete("Use CsvStyle.Escaped instead.")]
	Unquoted = 2,

	/// <summary>
	/// Interprets fields as if they are implicitly quoted. Delimiters and new lines within fields are preceded by an escape character.
	/// </summary>
	Escaped = 2,
}
