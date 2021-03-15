using System;
using Xunit;

namespace Sylvan
{
	public class SystemClockTests
	{
		[Fact]
		public void Test1()
		{
			var now = DateTime.UtcNow;
			var time = SystemClock.Instance.UtcNow;
			Assert.True(time >= now);
		}
	}
}
