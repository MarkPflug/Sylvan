﻿using System;
using System.Globalization;
using Xunit;

namespace Sylvan
{
	public class Iso8601DateTests
	{
		[Theory]
		[InlineData("2020-10-11", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11T12:13:14", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14Z", true, DateTimeKind.Utc)]
		[InlineData("2020-10-11T12:13:14Z", true, DateTimeKind.Utc)]
		[InlineData("2020-10-11 12:13:14.1", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14.12", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14.123", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14.12345", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14.123456", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14.1234567", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14.1234567Z", true, DateTimeKind.Utc)]
		[InlineData("2020-10-11 12:13:14.12345678Z", true, DateTimeKind.Utc)]
		[InlineData("2020-10-11 12:13:14.123456789", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11 12:13:14.123456789Z", true, DateTimeKind.Utc)]
		[InlineData("2020-10-11 23:59:59.99999999Z", true, DateTimeKind.Utc)]
		[InlineData("2020-10-11T12:13", true, DateTimeKind.Unspecified)]
		[InlineData("2020-10-11T12:13:14-07:00", true, DateTimeKind.Utc)]

		[InlineData("2020-10-11X12:13:14", false, DateTimeKind.Unspecified)]
		public void TryParseDateTime(string str, bool success, DateTimeKind expectedKind)
		{
			var result = IsoDate.TryParse(str, out DateTime value);
			Assert.Equal(success, result);
			if (success)
			{
				var expected = DateTime.Parse(str, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
				Assert.Equal(expected.Ticks, value.Ticks);
				Assert.Equal(expectedKind, value.Kind);
			}
		}

		[Theory]
		[InlineData("2020-10-11", true)]
		[InlineData("2020-10-11 12:13", true)]
		[InlineData("2020-10-11 12:13:14", true)]
		[InlineData("2020-10-11T12:13:14", true)]
		[InlineData("2020-10-11 12:13:14Z", true)]
		[InlineData("2020-10-11T12:13:14Z", true)]
		[InlineData("2020-10-11 12:13:14.1", true)]
		[InlineData("2020-10-11 12:13:14.12", true)]
		[InlineData("2020-10-11 12:13:14.123", true)]
		[InlineData("2020-10-11 12:13:14.12345", true)]
		[InlineData("2020-10-11 12:13:14.123456", true)]
		[InlineData("2020-10-11 12:13:14.1234567", true)]
		[InlineData("2020-10-11 12:13:14.1234567Z", true)]
		[InlineData("2020-10-11 12:13:14.12345678Z", true)]
		[InlineData("2020-10-11 12:13:14.123456789", true)]
		[InlineData("2020-10-11 12:13:14.123456789Z", true)]
		[InlineData("2020-10-11 23:59:59.99999999Z", true)]
		[InlineData("2020-10-11T12:13", true)]
		[InlineData("2020-10-11T12:13:14-07:00", true)]

		[InlineData("2020-10-11X12:13:14", false)]
		public void TryParseDateTimeOffset(string str, bool success)
		{
			var result = IsoDate.TryParse(str, out DateTimeOffset value);
			Assert.Equal(success, result);
			if (success)
			{
				var expected = DateTimeOffset.Parse(str);
				Assert.Equal(expected, value);
			}
		}

		[Theory]
		[InlineData("2020-10-11", true)]

		[InlineData("20-10-11", false)]
		[InlineData("2020-13-11", false)]
		[InlineData("2020-10-32", false)]
		public void TryParseDateOnly(string str, bool success)
		{
			var result = IsoDate.TryParse(str, out DateOnly value);
			Assert.Equal(success, result);
			if (success)
			{
				var expected = DateOnly.FromDateTime(DateTime.Parse(str));
				Assert.Equal(expected, value);
			}
		}
	}
}
