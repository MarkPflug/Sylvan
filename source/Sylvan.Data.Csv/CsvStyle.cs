using System;

namespace Sylvan.Data.Csv;

/// <summary>
/// The CSV quoting and escaping styles.
/// </summary>
public enum CsvStyle
{
	/// <summary>
	/// Parses using the standard RFC4180 mode.
	/// Malformed fields will produce a <see cref="CsvFormatException"/> during calls to <see cref="CsvDataReader.Read"/>.
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

	/// <summary>
	/// Parses CSV using lax quote handling where incorrectly quoted fields don't produce an error.
	/// In this mode a field will be parsed using the <see cref="Standard"/> mode, and when a (unescaped) closing quote is found, the remainder
	/// of the field will be parsed as if it were unquoted.
	/// </summary>
	Lax = 3,
}
