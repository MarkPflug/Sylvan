using System.Globalization;
using System.IO;

namespace Sylvan.CodeGeneration;

abstract partial class IdentifierStyle
{
	/// <summary>
	/// A "camelCase" identifier style.
	/// </summary>
	public static readonly IdentifierStyle CamelCase = new CamelCaseStyle();

	/// <summary>
	/// A "database_name" identifier style.
	/// </summary>
	public static readonly IdentifierStyle Database = new QuotedIdentifierStyle(CasingStyle.LowerCase, '_');

	/// <summary>
	/// A "human readable sentence" identifier style.
	/// </summary>
	public static readonly IdentifierStyle Sentence = new SentenceStyle(CasingStyle.TitleCase);
}

/// <summary>
/// The camel case identifier style.
/// </summary>
sealed class CamelCaseStyle : IdentifierStyle
{
	/// <inheritdoc/>
	public override string Convert(string str)
	{
		using var sw = new StringWriter();
		bool isUpper = IsAllUpper(str);

		bool first = true;
		char last = '\0';
		foreach (var segment in GetSegments(str))
		{
			for (int i = segment.Start; i < segment.End; i++)
			{
				var c = str[i];
				if (first)
				{
					c = char.ToLowerInvariant(c);
				}
				else
				{
					if (i == segment.Start)
					{
						if (char.IsDigit(c) && char.IsDigit(last))
							sw.Write('_');
						c = char.ToUpper(c, CultureInfo.InvariantCulture);
					}
					else
					{
						if (isUpper)
						{
							c = char.ToLower(c, CultureInfo.InvariantCulture);
						}
					}
				}
				sw.Write(c);
				last = c;
			}
			first = false;
		}
		return sw.ToString();
	}
}

/// <summary>
/// An identifier style that uses underscores to separate segments, commonly called "snake_case".
/// </summary>
sealed class UnderscoreStyle : IdentifierStyle
{
	readonly CasingStyle style;

	/// <summary>
	/// Constructs a new UnderscoreStyle.
	/// </summary>
	public UnderscoreStyle(CasingStyle style = CasingStyle.LowerCase)
	{
		this.style = style;
	}

	/// <inheritdoc/>
	public override string Convert(string str)
	{
		return Separated(str, style, '_');
	}
}

/// <summary>
/// An identifier style that uses dashes to separate segments, commonly called "kebab-case".
/// </summary>
sealed class DashStyle : IdentifierStyle
{
	readonly CasingStyle style;

	/// <summary>
	/// Constructs a new DashStyle.
	/// </summary>
	public DashStyle(CasingStyle style)
	{
		this.style = style;
	}

	/// <inheritdoc/>
	public override string Convert(string str)
	{
		return Separated(str, style, '-');
	}
}

/// <summary>
/// An identifier style that uses spaces to separate segments. This can be useful to convert identifiers to be presented in non-localized UI elements.
/// </summary>
sealed class SentenceStyle : IdentifierStyle
{
	readonly CasingStyle style;

	/// <summary>
	/// Constructs a new SentenceStyle.
	/// </summary>
	public SentenceStyle(CasingStyle style = CasingStyle.LowerCase)
	{
		this.style = style;
	}

	/// <inheritdoc/>
	public override string Convert(string str)
	{
		return Separated(str, style, ' ');
	}
}

/// <summary>
/// An identifier style commonly used by database languages like sql.
/// </summary>
sealed class QuotedIdentifierStyle : IdentifierStyle
{
	readonly CasingStyle style;
	readonly char separator;

	/// <summary>
	/// Constructs a new QuotedIdentifierStyle.
	/// </summary>
	public QuotedIdentifierStyle(CasingStyle style = CasingStyle.LowerCase, char separator = '_')
	{
		this.style = style;
		this.separator = separator;
	}

	/// <inheritdoc/>
	public override string Convert(string str)
	{
		return Separated(str, style, separator, '\"');
	}
}
