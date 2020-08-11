using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace Sylvan
{
	/// <summary>
	/// Provides conversions between different styles of identifiers.
	/// </summary>
	public abstract class IdentifierStyle
	{
		/// <summary>
		/// A "PascalCase" identifier style.
		/// </summary>
		public static readonly IdentifierStyle PascalCase = new PascalCaseStyle();

		/// <summary>
		/// A "camelCase" identifier style.
		/// </summary>
		public static readonly IdentifierStyle CamelCase = new CamelCaseStyle();

		/// <summary>
		/// A "database_name" identifier style".
		/// </summary>
		public static readonly IdentifierStyle Database = new QuotedIdentifierStyle(CasingStyle.LowerCase, '_');

		/// <summary>
		/// Converts a string to the given identifier style.
		/// </summary>
		public abstract string Convert(string str);

		internal string Separated(string str, CasingStyle segmentStyle, char separator = '\0', char quote = '\0')
		{
			if (str == null) throw new ArgumentNullException(nameof(str));

			using var sw = new StringWriter();
			if (quote != '\0')
			{
				sw.Write(quote);
			}
			bool isUpper = IsAllUpper(str);

			bool first = true;

			foreach (var segment in GetSegments(str))
			{
				if (!first)
				{
					if (separator != '\0')
						sw.Write(separator);
				}
				for (int i = segment.Start; i < segment.End; i++)
				{
					var c = str[i];

					if (i == segment.Start)
					{
						switch (segmentStyle)
						{
							case CasingStyle.LowerCase:
								c = char.ToLowerInvariant(c);
								break;
							case CasingStyle.TitleCase:
							case CasingStyle.UpperCase:
								c = char.ToUpperInvariant(c);
								break;
						}
					}
					else
					{
						switch (segmentStyle)
						{
							case CasingStyle.LowerCase:
								c = char.ToLowerInvariant(c);
								break;
							case CasingStyle.TitleCase:
								if (isUpper)
								{
									c = char.ToLowerInvariant(c);
								}
								break;
							case CasingStyle.UpperCase:
								c = char.ToUpperInvariant(c);
								break;
						}
					}
					sw.Write(c);
				}
				first = false;
			}
			if (quote != '\0')
			{
				sw.Write(quote);
			}
			return sw.ToString();
		}

		internal static IEnumerable<Range> GetSegments(string identifier)
		{
			int start = 0;
			int length = 0;

			for (var i = 0; i < identifier.Length; i++)
			{
				var c = identifier[i];
				var cat = char.GetUnicodeCategory(c);
				switch (cat)
				{
					case UnicodeCategory.UppercaseLetter:
						if (length > 0)
						{
							yield return new Range(start, length);
						}
						start = i;
						length = 1;
						for (int j = i + 1; j < identifier.Length; j++)
						{
							c = identifier[j];
							cat = char.GetUnicodeCategory(c);
							switch (cat)
							{
								case UnicodeCategory.UppercaseLetter:
									length++;
									break;
								case UnicodeCategory.LowercaseLetter:
									if (length > 1)
									{
										yield return new Range(start, length - 1);
										start = j - 1;
										i = j;
										length = 2;
									}
									goto done;
								default:
									yield return new Range(start, length);
									i = j;
									start = j;
									length = 0;
									goto done;
							}
						}
						i = identifier.Length;

					done:
						break;
					case UnicodeCategory.LowercaseLetter:
					case UnicodeCategory.DecimalDigitNumber:
						if (length == 0)
						{
							start = i;
						}
						length++;
						break;
					default:
						if (length > 0)
						{
							yield return new Range(start, length);
							length = 0;
						}
						break;
				}
			}
			if (length > 0)
			{
				yield return new Range(start, length);
			}
		}

		internal static bool IsAllUpper(string str)
		{
			for (int i = str.Length - 1; i >= 0; i--)
			{
				if (char.IsLower(str[i]))
					return false;
			}
			return true;
		}

		internal struct Range
		{
			public int Start { get; }
			public int Length { get; }

			public int End => Start + Length;

			public Range(int start, int length)
			{
				this.Start = start;
				this.Length = length;
			}
		}
	}

	/// <summary>
	/// The pascale identifier style.
	/// </summary>
	public sealed class PascalCaseStyle : IdentifierStyle
	{
		/// <inheritdoc/>
		public override string Convert(string str)
		{
			using var sw = new StringWriter();
			bool isUpper = IsAllUpper(str);

			foreach (var segment in GetSegments(str))
			{
				for (int i = segment.Start; i < segment.End; i++)
				{
					var c = str[i];
					if (i == segment.Start)
					{
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
				}
			}
			return sw.ToString();
		}
	}

	/// <summary>
	/// The camel case identifier style.
	/// </summary>
	public sealed class CamelCaseStyle : IdentifierStyle
	{
		/// <inheritdoc/>
		public override string Convert(string str)
		{
			using var sw = new StringWriter();
			bool isUpper = IsAllUpper(str);

			bool first = true;

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
				}
				first = false;
			}
			return sw.ToString();
		}
	}

	/// <summary>
	/// The casing style used within segments.
	/// </summary>
	public enum CasingStyle
	{
		/// <summary>
		/// Use the casing of the original identifier.
		/// </summary>
		Unchanged = 0,
		/// <summary>
		/// UpperCase every character.
		/// </summary>
		UpperCase,
		/// <summary>
		/// LowerCase every character.
		/// </summary>
		LowerCase,
		/// <summary>
		/// UpperCase first character, and lowercase the rest.
		/// </summary>
		TitleCase,
	}

	/// <summary>
	/// An identifier style that uses underscores to separate segments, commonly called "snake_case".
	/// </summary>
	public sealed class UnderscoreStyle : IdentifierStyle
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
	public sealed class DashStyle : IdentifierStyle
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
	public sealed class SentenceStyle : IdentifierStyle
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
			return Separated(str, style, '-');
		}
	}

	/// <summary>
	/// An identifier style commonly used by database languages like sql.
	/// </summary>
	public sealed class QuotedIdentifierStyle : IdentifierStyle
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
}
