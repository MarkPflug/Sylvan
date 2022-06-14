using System.Globalization;
using System.IO;

namespace Sylvan;

partial class IdentifierStyle
{
	/// <summary>
	/// A "PascalCase" identifier style.
	/// </summary>
	public static readonly IdentifierStyle PascalCase = new PascalCaseStyle();
}

/// <summary>
/// The pascale identifier style.
/// </summary>
sealed class PascalCaseStyle : IdentifierStyle
{
	/// <inheritdoc/>
	public override string Convert(string str)
	{
		using var sw = new StringWriter();
		bool isUpper = IsAllUpper(str);
		char last = '\0';
		foreach (var segment in GetSegments(str))
		{
			for (int i = segment.Start; i < segment.End; i++)
			{
				var c = str[i];
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
				sw.Write(c);
				last = c;
			}
		}
		return sw.ToString();
	}
}
