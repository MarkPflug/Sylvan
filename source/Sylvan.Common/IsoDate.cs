using System;

namespace Sylvan
{
	static class IsoDate
	{
		/// <summary>
		/// Parses and ISO8601 formatted date.
		/// </summary>
		/// <remarks> 
		/// <para>
		/// DateTime.Parse is quite slow when parsing date values because it handles
		/// a variety of formats and complexities introduced by culture and calendaring systems. 
		/// ParseExact is somewhat faster when given the "O" round-trippable format. 
		/// However, that requires providing a fully specified input with fractional seconds.
		/// </para>
		/// <para>
		/// IsoDate provides a much faster implementation specialized for ISO8601 formatted dates.
		/// It doesn't have to account for culture or calendaring systems, which allows it to 
		/// significantly faster than the DateTime.Parse implementations. 
		/// The ISO rules are relaxed to allow a space instead of a 'T' as the time separator.
		/// A comma can also be used as the fractional seconds separator, in place of a period.
		/// </para>
		/// </remarks>
		public static bool TryParse(ReadOnlySpan<char> s, out DateTime dt)
		{
			dt = default;
			bool success = false;
			bool isUtc = false;

			static int Digit(char c)
			{
				var d = c - '0';
				if (((uint)d) >= 10)
				{
					return -1;
				}
				return d;
			}

			// validate that at least the date part is present.
			if (s.Length < 10 || s[4] != '-' || s[7] != '-')
				goto exit;

			int digit;
			var year = 0;
			var month = 0;
			var day = 0;
			var hour = 0;
			var min = 0;
			var sec = 0;

			digit = Digit(s[0]);
			if (digit < 0)
				goto exit;
			year += digit * 1000;
			digit = Digit(s[1]);
			if (digit < 0)
				goto exit;
			year += digit * 100;

			digit = Digit(s[2]);
			if (digit < 0)
				goto exit;
			year += digit * 10;

			digit = Digit(s[3]);
			if (digit < 0)
				goto exit;
			year += digit * 1;

			digit = Digit(s[5]);
			if (digit < 0)
				goto exit;
			month += digit * 10;

			digit = Digit(s[6]);
			if (digit < 0)
				goto exit;
			month += digit * 1;

			digit = Digit(s[8]);
			if (digit < 0)
				goto exit;
			day += digit * 10;

			digit = Digit(s[9]);
			if (digit < 0)
				goto exit;
			day += digit * 1;

			if (s.Length == 10)
				goto date;

			if (s.Length < 19)
				goto exit;

			var sep = s[10];
			if ((sep != ' ' && sep != 'T') || s[13] != ':' || s[16] != ':')
				goto exit;

			digit = Digit(s[11]);
			if (digit < 0)
				goto exit;
			hour += digit * 10;

			digit = Digit(s[12]);
			if (digit < 0)
				goto exit;
			hour += digit * 1;

			digit = Digit(s[14]);
			if (digit < 0)
				goto exit;
			min += digit * 10;

			digit = Digit(s[15]);
			if (digit < 0)
				goto exit;
			min += digit * 1;

			digit = Digit(s[17]);
			if (digit < 0)
				goto exit;
			sec += digit * 10;

			digit = Digit(s[18]);
			if (digit < 0)
				goto exit;
			sec += digit * 1;

			date:
			dt = new DateTime(year, month, day, hour, min, sec, DateTimeKind.Unspecified);
			success = true;

			if (s.Length > 19) // handle fractional part
			{
				if (s.Length == 20)
				{
					if (s[19] == 'Z')
					{
						isUtc = true;
						goto exit;
					}
					else
					{
						success = false;
						goto exit;
					}
				}
				else
				{
					sep = s[19];
					if (sep != '.' && sep != ',')
					{
						success = false;
						goto exit;
					}

					var c = 0;
					var frac = 0;
					var i = 20;
					for (; i < s.Length && c < 7; i++, c++)
					{
						var d = s[i];
						digit = Digit(d);
						if (digit == -1)
						{
							if (d == 'Z')
							{
								if (i + 1 == s.Length)
								{
									isUtc = true;
									break;
								}
							}
							success = false;
							goto exit;
						}
						frac = frac * 10 + digit;
					}
					// if we had fewer than 7 digits
					// fill with zeros					
					while (c < 7)
					{
						frac *= 10;
						c++;
					}
					c = 0;
					for (; i < s.Length; i++, c++)
					{
						var d = s[i];
						digit = Digit(d);
						if (digit == -1)
						{
							if (d == 'Z')
							{
								if (i + 1 == s.Length)
								{
									isUtc = true;
									break;
								}
							}
							success = false;
							goto exit;
						}
						if (c == 0)
						{
							if (digit >= 5)
								frac++; // round up
						}
						else
						{
							// ignore any further trailing digits
						}
					}
					dt = dt.AddTicks(frac);
				}
			}

			exit:
			if (isUtc)
				dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
			return success;
		}
	}
}
