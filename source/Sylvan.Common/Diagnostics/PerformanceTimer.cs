using System;
using System.Diagnostics;
using System.Threading;

namespace Sylvan.Diagnostics
{
	public sealed class PerformanceTimer
	{
		public PerformanceTimer(string name)
		{
			this.Name = name;
		}

		int count;
		long stopwatchTicks;

		public string Name { get; }

		public int Count => count;

		long Ticks
		{
			get
			{
				Debug.Assert(stopwatchTicks < long.MaxValue / TimeSpan.TicksPerSecond);
				return this.stopwatchTicks * TimeSpan.TicksPerSecond / Stopwatch.Frequency;
			}
		}

		public TimeSpan TotalElasped
		{
			get
			{
				return new TimeSpan(Ticks);
			}
		}

		public TimeSpan AverageElapsed
		{
			get
			{
				return new TimeSpan(Ticks / count);
			}
		}

		public TimedSection Start()
		{
			return new TimedSection(this);
		}

		void End(long elapsed)
		{
			Interlocked.Increment(ref count);
			Interlocked.Add(ref this.stopwatchTicks, elapsed);
		}

		public override string ToString()
		{
			var count = this.count;
			var elapsed = this.TotalElasped;
			return $"Timer: {Name}, Count: {count}, Elapsed: {elapsed.ToString()}, Average: {AverageElapsed.ToString()}";
		}

		public readonly struct TimedSection : IDisposable
		{
			readonly PerformanceTimer timer;
			readonly long startTicks;

			public TimedSection(PerformanceTimer timer)
			{
				this.timer = timer;
				this.startTicks = Stopwatch.GetTimestamp();
			}
			public void Stop()
			{
				var elapsed = Stopwatch.GetTimestamp() - startTicks;
				timer.End(elapsed);
			}

			public void Dispose()
			{
				this.Stop();
			}
		}
	}
}
