// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sylvan;

/// <summary>
/// Provides ISO 8601 date parsing.
/// </summary>
static partial class IsoDate
{
	// This implementation is mostly copied from System.Text.Json internal code.
	// There are a few changes:
	// - handles chars instead of bytes, as the S.T.Json impl was utf-8 specific
	// - Allows ' ' in place of 'T' as time separator. Not ISO compliant, but common enough that I want to allow it.
	// - Allows ',' in place of '.' as fractional time separator, which ISO8601 apparently allows but often isn't supported.
	// - Rounds appropriately when more than 7 fracational second digits are parsed.

	const int DateTimeParseNumFractionDigits = 16;
	const int DateTimeNumFractionDigits = 7;
	const int MaxDateTimeUtcOffsetHours = 14;
	const int MaxDateTimeFraction = 10_000_000;

	private struct DateTimeParseData
	{
		public int Year;
		public int Month;
		public int Day;
		public int Hour;
		public int Minute;
		public int Second;
		public int Fraction; // This value should never be greater than 9_999_999.
		public int OffsetHours;
		public int OffsetMinutes;
		public bool OffsetNegative => OffsetToken == '-';
		public char OffsetToken;
	}

	/// <summary>
	/// Parse the given <paramref name="source"/> as extended ISO 8601 format.
	/// </summary>
	/// <param name="source">The source to parse.</param>
	/// <param name="value">The parsed <see cref="DateTime"/> if successful.</param>
	/// <returns>"true" if successfully parsed.</returns>
	public static bool TryParse(ReadOnlySpan<char> source, out DateTime value)
	{
		if (!TryParseDateTimeOffset(source, out DateTimeParseData parseData))
		{
			value = default;
			return false;
		}

		if (parseData.OffsetToken == 'Z')
		{
			return TryCreateDateTime(parseData, DateTimeKind.Utc, out value);
		}
		else if (parseData.OffsetToken == '+' || parseData.OffsetToken == '-')
		{
			if (!TryCreateDateTimeOffset(ref parseData, out DateTimeOffset dateTimeOffset))
			{
				value = default;
				return false;
			}

			value = dateTimeOffset.UtcDateTime;
			return true;
		}

		return TryCreateDateTime(parseData, DateTimeKind.Unspecified, out value);
	}

	/// <summary>
	/// Parse the given <paramref name="source"/> as extended ISO 8601 format.
	/// </summary>
	/// <param name="source">The source to parse.</param>
	/// <param name="value">The parsed <see cref="DateTimeOffset"/> if successful.</param>
	/// <returns>"true" if successfully parsed.</returns>
	public static bool TryParse(ReadOnlySpan<char> source, out DateTimeOffset value)
	{
		if (!TryParseDateTimeOffset(source, out DateTimeParseData parseData))
		{
			value = default;
			return false;
		}

		if (parseData.OffsetToken == 'Z' || // Same as specifying an offset of "+00:00", except that DateTime's Kind gets set to UTC rather than Local
			parseData.OffsetToken == '+' || parseData.OffsetToken == '-')
		{
			return TryCreateDateTimeOffset(ref parseData, out value);
		}

		// No offset, attempt to read as local time.
		return TryCreateDateTimeOffsetInterpretingDataAsLocalTime(parseData, out value);
	}


#if NET6_0_OR_GREATER
	/// <summary>
	/// Parse the given <paramref name="source"/> as extended ISO 8601 format.
	/// </summary>
	/// <param name="source">The source to parse.</param>
	/// <param name="value">The parsed <see cref="DateOnly"/> if successful.</param>
	/// <returns>"true" if successfully parsed.</returns>
	public static bool TryParse(ReadOnlySpan<char> source, out DateOnly value)
	{
		value = default;

		if (source.Length != 10 || source[4] != '-' || source[7] != '-') return false;

		uint digit1 = source[0] - (uint)'0';
		uint digit2 = source[1] - (uint)'0';
		uint digit3 = source[2] - (uint)'0';
		uint digit4 = source[3] - (uint)'0';

		if (digit1 > 9 || digit2 > 9 || digit3 > 9 || digit4 > 9)
		{
			return false;
		}

		int year = (int)(digit1 * 1000 + digit2 * 100 + digit3 * 10 + digit4);
		int month = 0;
		int day = 0;

		if (!TryGetNextTwoDigits(source.Slice(start: 5, length: 2), ref month)
			|| !TryGetNextTwoDigits(source.Slice(start: 8, length: 2), ref day))
		{
			return false;
		}

		if ((uint)year > 9999 || month > 12)
		{
			return false;
		}

		var maxDaysInMonth = DateTime.DaysInMonth(year, month);
		if (day > maxDaysInMonth)
		{
			return false;
		}

		value = new DateOnly(year, month, day);
		return true;
	}

#endif

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static bool IsDigit(int i)
	{
		return (uint)(i - '0') <= ('9' - '0');
	}

	/// <summary>
	/// ISO 8601 date time parser (ISO 8601-1:2019).
	/// </summary>
	/// <param name="source">The date/time to parse in UTF-8 format.</param>
	/// <param name="parseData">The parsed <see cref="DateTimeParseData"/> for the given <paramref name="source"/>.</param>
	/// <remarks>
	/// Supports extended calendar date (5.2.2.1) and complete (5.4.2.1) calendar date/time of day
	/// representations with optional specification of seconds and fractional seconds.
	///
	/// Times can be explicitly specified as UTC ("Z" - 5.3.3) or offsets from UTC ("+/-hh:mm" 5.3.4.2).
	/// If unspecified they are considered to be local per spec.
	///
	/// Examples: (TZD is either "Z" or hh:mm offset from UTC)
	///
	///  YYYY-MM-DD               (eg 1997-07-16)
	///  YYYY-MM-DDThh:mm         (eg 1997-07-16T19:20)
	///  YYYY-MM-DDThh:mm:ss      (eg 1997-07-16T19:20:30)
	///  YYYY-MM-DDThh:mm:ss.s    (eg 1997-07-16T19:20:30.45)
	///  YYYY-MM-DDThh:mmTZD      (eg 1997-07-16T19:20+01:00)
	///  YYYY-MM-DDThh:mm:ssTZD   (eg 1997-07-16T19:20:3001:00)
	///  YYYY-MM-DDThh:mm:ss.sTZD (eg 1997-07-16T19:20:30.45Z)
	///
	/// Generally speaking we always require the "extended" option when one exists (3.1.3.5).
	/// The extended variants have separator characters between components ('-', ':', '.', etc.).
	/// Spaces are not permitted.
	/// </remarks>
	/// <returns>"true" if successfully parsed.</returns>
	static bool TryParseDateTimeOffset(ReadOnlySpan<char> source, out DateTimeParseData parseData)
	{
		parseData = default;

		// too short datetime
		if (source.Length < 10)
			return false;

		// Parse the calendar date
		// -----------------------
		// ISO 8601-1:2019 5.2.2.1b "Calendar date complete extended format"
		//  [dateX] = [year]["-"][month]["-"][day]
		//  [year]  = [YYYY] [0000 - 9999] (4.3.2)
		//  [month] = [MM] [01 - 12] (4.3.3)
		//  [day]   = [DD] [01 - 28, 29, 30, 31] (4.3.4)
		//
		// Note: 5.2.2.2 "Representations with reduced precision" allows for
		// just [year]["-"][month] (a) and just [year] (b), but we currently
		// don't permit it.

		{
			uint digit1 = source[0] - (uint)'0';
			uint digit2 = source[1] - (uint)'0';
			uint digit3 = source[2] - (uint)'0';
			uint digit4 = source[3] - (uint)'0';

			if (digit1 > 9 || digit2 > 9 || digit3 > 9 || digit4 > 9)
			{
				return false;
			}

			parseData.Year = (int)(digit1 * 1000 + digit2 * 100 + digit3 * 10 + digit4);
		}

		if (source[4] != '-'
			|| !TryGetNextTwoDigits(source.Slice(start: 5, length: 2), ref parseData.Month)
			|| source[7] != '-'
			|| !TryGetNextTwoDigits(source.Slice(start: 8, length: 2), ref parseData.Day))
		{
			return false;
		}

		// We now have YYYY-MM-DD [dateX]
		if (source.Length == 10)
		{
			// Just a calendar date
			return true;
		}

		// Parse the time of day
		// ---------------------
		//
		// ISO 8601-1:2019 5.3.1.2b "Local time of day complete extended format"
		//  [timeX]   = ["T"][hour][":"][min][":"][sec]
		//  [hour]    = [hh] [00 - 23] (4.3.8a)
		//  [minute]  = [mm] [00 - 59] (4.3.9a)
		//  [sec]     = [ss] [00 - 59, 60 with a leap second] (4.3.10a)
		//
		// ISO 8601-1:2019 5.3.3 "UTC of day"
		//  [timeX]["Z"]
		//
		// ISO 8601-1:2019 5.3.4.2 "Local time of day with the time shift between
		// local time scale and UTC" (Extended format)
		//
		//  [shiftX] = ["+"|"-"][hour][":"][min]
		//
		// Notes:
		//
		// "T" is optional per spec, but _only_ when times are used alone. In our
		// case, we're reading out a complete date & time and as such require "T".
		// (5.4.2.1b).
		//
		// For [timeX] We allow seconds to be omitted per 5.3.1.3a "Representations
		// with reduced precision". 5.3.1.3b allows just specifying the hour, but
		// we currently don't permit this.
		//
		// Decimal fractions are allowed for hours, minutes and seconds (5.3.14).
		// We only allow fractions for seconds currently. Lower order components
		// can't follow, i.e. you can have T23.3, but not T23.3:04. There must be
		// one digit, but the max number of digits is implemenation defined. We
		// currently allow up to 16 digits of fractional seconds only. While we
		// support 16 fractional digits we only parse the first seven, anything
		// past that is considered a zero. This is to stay compatible with the
		// DateTime implementation which is limited to this resolution.

		if (source.Length < 16)
		{
			// Source does not have enough characters for YYYY-MM-DDThh:mm
			return false;
		}

		// Parse THH:MM (e.g. "T10:32")
		var timeSep = source[10];
		if (!(timeSep == 'T' || timeSep == ' ') || source[13] != ':'
			|| !TryGetNextTwoDigits(source.Slice(start: 11, length: 2), ref parseData.Hour)
			|| !TryGetNextTwoDigits(source.Slice(start: 14, length: 2), ref parseData.Minute))
		{
			return false;
		}

		// We now have YYYY-MM-DDThh:mm
		if (source.Length == 16)
		{
			return true;
		}

		char curChar = source[16];
		int sourceIndex = 17;

		// Either a TZD ['Z'|'+'|'-'] or a seconds separator [':'] is valid at this point
		switch (curChar)
		{
			case 'Z':
				parseData.OffsetToken = curChar;
				return sourceIndex == source.Length;
			case '+':
			case '-':
				parseData.OffsetToken = curChar;
				return ParseOffset(ref parseData, source.Slice(sourceIndex));
			case ':':
				break;
			default:
				return false;
		}

		// Try reading the seconds
		if (source.Length < 19
			|| !TryGetNextTwoDigits(source.Slice(start: 17, length: 2), ref parseData.Second))
		{
			return false;
		}

		// We now have YYYY-MM-DDThh:mm:ss
		if (source.Length == 19)
		{
			return true;
		}

		curChar = source[19];
		sourceIndex = 20;

		// Either a TZD ['Z'|'+'|'-'] or a seconds decimal fraction separator ['.' (or ',')] is valid at this point
		switch (curChar)
		{
			case 'Z':
				parseData.OffsetToken = curChar;
				return sourceIndex == source.Length;
			case '+':
			case '-':
				parseData.OffsetToken = curChar;
				return ParseOffset(ref parseData, source.Slice(sourceIndex));
			case '.':
			case ',':
				break;
			default:
				return false;
		}

		// Source does not have enough characters for second fractions (i.e. ".s")
		// YYYY-MM-DDThh:mm:ss.s
		if (source.Length < 21)
		{
			return false;
		}

		// Parse fraction.
		{
			int numDigitsRead = 0;
			int fractionEnd = Math.Min(sourceIndex + DateTimeParseNumFractionDigits, source.Length);
			while (sourceIndex < fractionEnd && IsDigit(curChar = source[sourceIndex]))
			{
				if (numDigitsRead < DateTimeNumFractionDigits)
				{
					parseData.Fraction = (parseData.Fraction * 10) + (int)(curChar - (uint)'0');
				}
				else
				{
					if (numDigitsRead == DateTimeNumFractionDigits)
					{
						// DateTime only allows 7 digits, but we can parse up to 16.
						// So, if the 8th digit is > 5, round up.
						var d = curChar - '0';
						if (d >= 5)
							parseData.Fraction++;
					}
				}
				numDigitsRead++;
				sourceIndex++;
			}

			if (parseData.Fraction != 0)
			{
				while (numDigitsRead < DateTimeNumFractionDigits)
				{
					parseData.Fraction *= 10;
					numDigitsRead++;
				}
			}
		}

		// We now have YYYY-MM-DDThh:mm:ss.s
		if (sourceIndex == source.Length)
		{
			return true;
		}

		curChar = source[sourceIndex++];

		// TZD ['Z'|'+'|'-'] is valid at this point
		switch (curChar)
		{
			case 'Z':
				parseData.OffsetToken = curChar;
				return sourceIndex == source.Length;
			case '+':
			case '-':
				parseData.OffsetToken = curChar;
				return ParseOffset(ref parseData, source.Slice(sourceIndex));
			default:
				return false;
		}

		static bool ParseOffset(ref DateTimeParseData parseData, ReadOnlySpan<char> offsetData)
		{
			// Parse the hours for the offset
			if (offsetData.Length < 2
				|| !TryGetNextTwoDigits(offsetData.Slice(0, 2), ref parseData.OffsetHours))
			{
				return false;
			}

			// We now have YYYY-MM-DDThh:mm:ss.s+|-hh

			if (offsetData.Length == 2)
			{
				// Just hours offset specified
				return true;
			}

			// Ensure we have enough for ":mm"
			if (offsetData.Length != 5
				|| offsetData[2] != ':'
				|| !TryGetNextTwoDigits(offsetData.Slice(3), ref parseData.OffsetMinutes))
			{
				return false;
			}

			return true;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static bool TryGetNextTwoDigits(ReadOnlySpan<char> source, ref int value)
	{
		Debug.Assert(source.Length == 2);

		uint digit1 = source[0] - (uint)'0';
		uint digit2 = source[1] - (uint)'0';

		if (digit1 > 9 || digit2 > 9)
		{
			value = default;
			return false;
		}

		value = (int)(digit1 * 10 + digit2);
		return true;
	}

	// The following methods are borrowed verbatim from src/Common/src/CoreLib/System/Buffers/Text/Utf8Parser/Utf8Parser.Date.Helpers.cs

	/// <summary>
	/// Overflow-safe DateTimeOffset factory.
	/// </summary>
	static bool TryCreateDateTimeOffset(DateTime dateTime, ref DateTimeParseData parseData, out DateTimeOffset value)
	{
		if (((uint)parseData.OffsetHours) > MaxDateTimeUtcOffsetHours)
		{
			value = default;
			return false;
		}

		if (((uint)parseData.OffsetMinutes) > 59)
		{
			value = default;
			return false;
		}

		if (parseData.OffsetHours == MaxDateTimeUtcOffsetHours && parseData.OffsetMinutes != 0)
		{
			value = default;
			return false;
		}

		long offsetTicks = (((long)parseData.OffsetHours) * 3600 + ((long)parseData.OffsetMinutes) * 60) * TimeSpan.TicksPerSecond;
		if (parseData.OffsetNegative)
		{
			offsetTicks = -offsetTicks;
		}

		try
		{
			value = new DateTimeOffset(ticks: dateTime.Ticks, offset: new TimeSpan(ticks: offsetTicks));
		}
		catch (ArgumentOutOfRangeException)
		{
			// If we got here, the combination of the DateTime + UTC offset strayed outside the 1..9999 year range. This case seems rare enough
			// that it's better to catch the exception rather than replicate DateTime's range checking (which it's going to do anyway.)
			value = default;
			return false;
		}

		return true;
	}

	/// <summary>
	/// Overflow-safe DateTimeOffset factory.
	/// </summary>
	static bool TryCreateDateTimeOffset(ref DateTimeParseData parseData, out DateTimeOffset value)
	{
		if (!TryCreateDateTime(parseData, kind: DateTimeKind.Unspecified, out DateTime dateTime))
		{
			value = default;
			return false;
		}

		if (!TryCreateDateTimeOffset(dateTime: dateTime, ref parseData, out value))
		{
			value = default;
			return false;
		}

		return true;
	}

	/// <summary>
	/// Overflow-safe DateTimeOffset/Local time conversion factory.
	/// </summary>
	static bool TryCreateDateTimeOffsetInterpretingDataAsLocalTime(DateTimeParseData parseData, out DateTimeOffset value)
	{
		if (!TryCreateDateTime(parseData, DateTimeKind.Local, out DateTime dateTime))
		{
			value = default;
			return false;
		}

		try
		{
			value = new DateTimeOffset(dateTime);
		}
		catch (ArgumentOutOfRangeException)
		{
			// If we got here, the combination of the DateTime + UTC offset strayed outside the 1..9999 year range. This case seems rare enough
			// that it's better to catch the exception rather than replicate DateTime's range checking (which it's going to do anyway.)
			value = default;
			return false;
		}

		return true;
	}

	static bool TryCreateDateTime(DateTimeParseData parseData, DateTimeKind kind, out DateTime value)
	{
		if (parseData.Year == 0)
		{
			value = default;
			return false;
		}

		Debug.Assert(parseData.Year <= 9999); // All of our callers to date parse the year from fixed 4-digit fields so this value is trusted.

		if ((((uint)parseData.Month) - 1) >= 12)
		{
			value = default;
			return false;
		}

		uint dayMinusOne = ((uint)parseData.Day) - 1;
		if (dayMinusOne >= 28 && dayMinusOne >= DateTime.DaysInMonth(parseData.Year, parseData.Month))
		{
			value = default;
			return false;
		}

		if (((uint)parseData.Hour) > 23)
		{
			value = default;
			return false;
		}

		if (((uint)parseData.Minute) > 59)
		{
			value = default;
			return false;
		}

		// This needs to allow leap seconds when appropriate.
		// See https://github.com/dotnet/runtime/issues/30135.
		if (((uint)parseData.Second) > 59)
		{
			value = default;
			return false;
		}

		// All of our callers to date parse the fraction from fixed 7-digit fields so this value is trusted.
		Debug.Assert(parseData.Fraction >= 0 && parseData.Fraction <= MaxDateTimeFraction);

		int[] days = DateTime.IsLeapYear(parseData.Year) ? DaysToMonth366 : DaysToMonth365;
		int yearMinusOne = parseData.Year - 1;
		int totalDays = (yearMinusOne * 365) + (yearMinusOne / 4) - (yearMinusOne / 100) + (yearMinusOne / 400) + days[parseData.Month - 1] + parseData.Day - 1;
		long ticks = totalDays * TimeSpan.TicksPerDay;
		int totalSeconds = (parseData.Hour * 3600) + (parseData.Minute * 60) + parseData.Second;
		ticks += totalSeconds * TimeSpan.TicksPerSecond;
		ticks += parseData.Fraction;
		value = new DateTime(ticks: ticks, kind: kind);

		return true;
	}

	static string GetString(Span<char> buffer)
	{
#if NETSTANDARD2_0
		throw new NotImplementedException();
#else
		return new string(buffer);
#endif
	}

	// "yyyy-MM-ddTHH:mm:ss.fffffff+HH:mm"
	internal const int MaxDateLength = 35;

	public static string ToStringIso(DateTime value)
	{
		Span<char> buffer = stackalloc char[MaxDateLength];
		bool known = false;
		int offset = 0;
		switch (value.Kind)
		{
			case DateTimeKind.Local:
				offset = (int)(TimeZoneInfo.Local.GetUtcOffset(value).Ticks / TimeSpan.TicksPerMinute);
				known = true;
				break;
			case DateTimeKind.Utc:
				known = true;
				break;
		}

		var len = FormatIsoInternal(value.Ticks, offset, known, buffer);
		return GetString(buffer.Slice(0, len));
	}

	public static bool TryFormatIso(DateTime value, Span<char> buffer, out int charsWritten)
	{

		if (buffer.Length < MaxDateLength)
		{
			charsWritten = 0;
			return false;
		}
		bool known = false;
		int offset = 0;
		switch (value.Kind)
		{
			case DateTimeKind.Local:
				offset = (int)(TimeZoneInfo.Local.GetUtcOffset(value).Ticks / TimeSpan.TicksPerMinute);
				known = true;
				break;
			case DateTimeKind.Utc:
				known = true;
				break;
		}

		charsWritten = FormatIsoInternal(value.Ticks, offset, known, buffer);
		return true;
	}

	public static string ToStringIso(DateTimeOffset value)
	{
		Span<char> buffer = stackalloc char[MaxDateLength];
		var offsetMinutes = (int)(value.Offset.Ticks / TimeSpan.TicksPerMinute);
		var length = FormatIsoInternal(value.Ticks, offsetMinutes, true, buffer);
		return GetString(buffer.Slice(0, length));
	}

#if NET6_0_OR_GREATER

	internal const int MaxDateOnlyLength = 10;

	public static string ToStringIso(DateOnly value)
	{
		Span<char> buffer = stackalloc char[MaxDateOnlyLength];
		var success = TryFormatIso(value, buffer, out int length);
		Debug.Assert(success);
		return GetString(buffer.Slice(0, length));
	}

	public static bool TryFormatIso(DateOnly value, Span<char> buffer, out int charsWritten)
	{
		if (buffer.Length < MaxDateOnlyLength)
		{
			charsWritten = 0;
			return false;
		}
		charsWritten = FormatIsoInternal(value.DayNumber * TimeSpan.TicksPerDay, 0, false, buffer);
		return true;
	}

#endif
	public static bool TryFormatIso(DateTimeOffset value, Span<char> buffer, out int charsWritten)
	{
		if (buffer.Length < MaxDateLength)
		{
			charsWritten = 0;
			return false;
		}
		charsWritten = FormatIsoInternal(value.Ticks, (int)(value.Offset.Ticks / TimeSpan.TicksPerMinute), true, buffer);
		return true;
	}

	static int FormatIsoInternal(long ticks, int offsetMinutes, bool offsetKnown, Span<char> buffer)
	{
		int len = 10;

		// n = number of days since 1/1/0001
		var n = Math.DivRem(ticks, TimeSpan.TicksPerDay, out long remain);

		// y400 = number of whole 400-year periods since 1/1/0001
		var y400 = n / DaysPer400Years;
		// n = day number within 400-year period
		n -= y400 * DaysPer400Years;
		// y100 = number of whole 100-year periods within 400-year period
		var y100 = n / DaysPer100Years;
		// Last 100-year period has an extra day, so decrement result if 4
		if (y100 == 4) y100 = 3;
		// n = day number within 100-year period
		n -= y100 * DaysPer100Years;
		// y4 = number of whole 4-year periods within 100-year period
		var y4 = n / DaysPer4Years;
		// n = day number within 4-year period
		n -= y4 * DaysPer4Years;
		// y1 = number of whole years within 4-year period
		var y1 = n / DaysPerYear;
		// Last year has an extra day, so decrement result if 4
		if (y1 == 4) y1 = 3;
		// compute year
		var year = (int)(y400 * 400 + y100 * 100 + y4 * 4 + y1 + 1);
		// n = day number within year
		n -= y1 * DaysPerYear;
		// dayOfYear = n + 1;
		// Leap year calculation looks different from IsLeapYear since y1, y4,
		// and y100 are relative to year 1, not year 0
		int[] days = y1 == 3 && (y4 != 24 || y100 == 3) ? DaysToMonth366 : DaysToMonth365;
		// All months have less than 32 days, so n >> 5 is a good conservative
		// estimate for the month
		var m = (n >> 5) + 1;
		// m = 1-based month number
		while (n >= days[m]) m++;
		// compute month and day
		var month = (int)m;
		var day = (int)(n - days[m - 1] + 1);

		WriteFourDigits((uint)year, buffer, 0);
		buffer[4] = '-';
		WriteTwoDigits((uint)month, buffer, 5);
		buffer[7] = '-';
		WriteTwoDigits((uint)day, buffer, 8);

		if (offsetKnown || remain != 0)
		{
			len = 19;
			var seconds = remain / TimeSpan.TicksPerSecond;
			var tick = (int)(remain - (seconds * TimeSpan.TicksPerSecond));
			var minutes = seconds / 60;
			var second = (int)(seconds - (minutes * 60));
			var hours = minutes / 60;
			var minute = (int)(minutes - (hours * 60));
			var hour = (int)((uint)hours % 24);


			buffer[10] = 'T';
			WriteTwoDigits((uint)hour, buffer, 11);
			buffer[13] = ':';
			WriteTwoDigits((uint)minute, buffer, 14);
			buffer[16] = ':';
			WriteTwoDigits((uint)second, buffer, 17);
			if (tick > 0)
			{
				buffer[19] = '.';
				WriteDigits((uint)tick, buffer.Slice(20, 7));
				len = 27;
			}

			if (offsetKnown)
			{
				if (offsetMinutes == 0)
				{
					buffer[len] = 'Z';
					len++;
				}
				else
				{
					char tzSep;
					if (offsetMinutes < 0)
					{
						tzSep = '-';
						offsetMinutes = -offsetMinutes;
					}
					else
					{
						tzSep = '+';
					}
					int value = Math.DivRem(offsetMinutes, 60, out var r);
					WriteTwoDigits((uint)r, buffer, len + 4);
					buffer[len + 3] = ':';
					WriteTwoDigits((uint)value, buffer.Slice(len + 1));
					buffer[len] = tzSep;
					len += 6;
				}
			}
		}
		return len;
	}

	/// <summary>
	/// Writes a value [ 0000 .. 9999 ] to the buffer starting at the specified offset.
	/// This method performs best when the starting index is a constant literal.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void WriteFourDigits(uint value, Span<char> buffer, int startingIndex = 0)
	{
		Debug.Assert(value <= 9999);

		uint temp = '0' + value;
		value /= 10;
		buffer[startingIndex + 3] = (char)(temp - (value * 10));

		temp = '0' + value;
		value /= 10;
		buffer[startingIndex + 2] = (char)(temp - (value * 10));

		temp = '0' + value;
		value /= 10;
		buffer[startingIndex + 1] = (char)(temp - (value * 10));

		buffer[startingIndex] = (char)('0' + value);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void WriteTwoDigits(uint value, Span<char> buffer, int startingIndex = 0)
	{
		Debug.Assert(value <= 99);
		uint temp = '0' + value;
		value /= 10;
		buffer[startingIndex + 1] = (char)(temp - (value * 10));

		buffer[startingIndex] = (char)('0' + value);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void WriteDigits(uint value, Span<char> buffer)
	{
		for (int i = buffer.Length - 1; i >= 1; i--)
		{
			uint temp = '0' + value;
			value /= 10;
			buffer[i] = (char)(temp - (value * 10));
		}

		Debug.Assert(value < 10);
		buffer[0] = (char)('0' + value);
	}

	const int DaysPerYear = 365;
	const int DaysPer4Years = DaysPerYear * 4 + 1;
	const int DaysPer100Years = DaysPer4Years * 25 - 1;
	const int DaysPer400Years = DaysPer100Years * 4 + 1;

	static readonly int[] DaysToMonth365 = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 };
	static readonly int[] DaysToMonth366 = { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 };
}
