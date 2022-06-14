using System;
using System.Threading;
using Xunit;

namespace Sylvan.Diagnostics;

public class NamedTimerTests
{
	public static PerformanceTimer MyTimer = new PerformanceTimer("MyTimer");

	[Fact]
	public void Test1()
	{
		Assert.True(MyTimer.Count == 0);
		Assert.True(MyTimer.TotalElasped == TimeSpan.Zero);

		using (MyTimer.Start())
		{
			Thread.Sleep(100);
			Assert.True(MyTimer.Count == 0);
		}

		Assert.True(MyTimer.TotalElasped > TimeSpan.FromMilliseconds(100));
		Assert.True(MyTimer.Count == 1);
	}
}
