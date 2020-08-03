using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace Sylvan
{
	public abstract class IdentifierStyle
	{
		public static readonly IdentifierStyle PascalCase = new PascalCaseStyle();
		public static readonly IdentifierStyle CamelCase = new CamelCaseStyle();
		public static readonly IdentifierStyle Database = new QuotedIdentifierStyle(CasingStyle.LowerCase);

		public abstract string Convert(string str);

		public static IEnumerable<Range> GetSegments(string identifier)
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

		public static bool IsAllUpper(string str)
		{
			for (int i = str.Length - 1; i >= 0; i--)
			{
				if (char.IsLower(str[i]))
					return false;
			}
			return true;
		}

		public struct Range
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

	public sealed class PascalCaseStyle : IdentifierStyle
	{
		public override string Convert(string str)
		{
			var sw = new StringWriter();
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

	public sealed class CamelCaseStyle : IdentifierStyle
	{
		public override string Convert(string str)
		{
			var sw = new StringWriter();
			bool isUpper = IsAllUpper(str);

			bool first = true;

			foreach (var segment in GetSegments(str))
			{
				for (int i = segment.Start; i < segment.End; i++)
				{
					var c = str[i];
					if (first)
					{
						c = char.ToLower(c);
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

	public abstract class SeparatedStyle : IdentifierStyle
	{
		readonly char quote;
		readonly char separator;
		readonly CasingStyle segmentStyle;

		public SeparatedStyle(CasingStyle segmentStyle, char separator = '\0', char quote = '\0')
		{
			this.quote = quote;
			this.separator = separator;
			this.segmentStyle = segmentStyle;
		}

		public override string Convert(string str)
		{
			var sw = new StringWriter();
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
						switch (this.segmentStyle)
						{
							case CasingStyle.LowerCase:
								c = char.ToLower(c);
								break;
							case CasingStyle.TitleCase:
							case CasingStyle.UpperCase:
								c = char.ToUpper(c);
								break;
						}
					}
					else
					{
						switch (this.segmentStyle)
						{
							case CasingStyle.LowerCase:
								c = char.ToLower(c);
								break;
							case CasingStyle.TitleCase:
								if (isUpper)
								{
									c = char.ToLower(c);
								}
								break;
							case CasingStyle.UpperCase:
								c = char.ToUpper(c);
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
	}

	public sealed class UnderscoreStyle : SeparatedStyle
	{
		public UnderscoreStyle(CasingStyle style) : base(style, '_') { }
	}

	public sealed class DashStyle : SeparatedStyle
	{
		public DashStyle(CasingStyle style) : base(style, '-') { }
	}

	public sealed class SentenceStyle : SeparatedStyle
	{
		public SentenceStyle(CasingStyle style) : base(style, ' ') { }
	}

	public sealed class QuotedIdentifierStyle : SeparatedStyle
	{
		public QuotedIdentifierStyle(CasingStyle style, char separator = '_', char quote = '\"') : base(style, separator, quote) { }
	}
}
