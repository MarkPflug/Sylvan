using System;
using System.Globalization;
using Xunit;

namespace Sylvan
{
	public class Iso8601DateTests
	{
		[Theory]
		[InlineData("2020-10-11", true)]
		[InlineData("2020-10-11 12:13:14", true)]
		[InlineData("2020-10-11T12:13:14", true)]
		[InlineData("2020-10-11 12:13:14Z", true)]
		[InlineData("2020-10-11T12:13:14Z", true)]
		[InlineData("2020-10-11 12:13:14.1", true)]
		[InlineData("2020-10-11 12:13:14.12", true)]
		[InlineData("2020-10-11 12:13:14.123", true)]
		[InlineData("2020-10-11 12:13:14.1235", true)]
		[InlineData("2020-10-11 12:13:14.12356", true)]
		[InlineData("2020-10-11 12:13:14.123567", true)]
		[InlineData("2020-10-11 12:13:14.1235678", true)]
		[InlineData("2020-10-11 12:13:14.1235678Z", true)]
		[InlineData("2020-10-11 12:13:14.12356789", true)]
		[InlineData("2020-10-11 12:13:14.12356789Z", true)]
		[InlineData("2020-10-11 12:13", false)]
		[InlineData("2020-10-11 12:13:14-07:00", false)]
		[InlineData("2020-10-11Z12:13:14", false)]
		public void TryParse(string str, bool success)
		{
			var result = IsoDate.TryParse(str, out var value);
			Assert.Equal(success, result);
			if (success)
			{
				var expected = DateTime.Parse(str, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
				Assert.Equal(expected.Ticks, value.Ticks);
				Assert.Equal(expected.Kind, value.Kind);
			}
		}
	}
}
