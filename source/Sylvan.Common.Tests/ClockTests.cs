using Sylvan.Diagnostics;
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
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

		[Fact]
		public void F()
		{
			var oh = PerformanceTimer.CalculateOverhead();
			Assert.True(oh > 5e-8, oh.ToString());
		}


		[DllImport("kernel32.dll", ExactSpelling = true)]
		static extern bool QueryPerformanceFrequency(out long lpFrequency);

		[DllImport("kernel32.dll", ExactSpelling = true)]
		static extern bool QueryPerformanceCounter(out long lpPerformanceCount);

		[Fact]
		public void qq()
		{
			var ff = Stopwatch.Frequency;
			var hr = Stopwatch.IsHighResolution;

			long freq;
			var b1 = QueryPerformanceFrequency(out freq);

			long c;
			var b2 = QueryPerformanceCounter(out c);
		}
	}
}
