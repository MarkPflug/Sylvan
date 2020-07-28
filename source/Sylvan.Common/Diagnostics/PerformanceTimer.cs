using System;
using System.Diagnostics;
using System.Threading;

namespace Sylvan.Diagnostics
{
	/// <summary>
	/// A timer for measuring application performance.
	/// </summary>
	public sealed class PerformanceTimer
	{
		/// <summary>
		/// Constructs a new PerformanceTimer.
		/// </summary>
		public PerformanceTimer(string name)
		{
			this.Name = name;
		}

		int count;
		long stopwatchTicks;

		/// <summary>
		/// Gets the name of the timer.
		/// </summary>
		public string Name { get; }

		/// <summary>
		/// Gets the number of times the timer was triggered.
		/// </summary>
		public int Count => count;

		long Ticks
		{
			get
			{
				Debug.Assert(stopwatchTicks < long.MaxValue / TimeSpan.TicksPerSecond);
				return this.stopwatchTicks * TimeSpan.TicksPerSecond / Stopwatch.Frequency;
			}
		}

		/// <summary>
		/// Gets the total elapsed time the timer was active.
		/// </summary>
		public TimeSpan TotalElasped
		{
			get
			{
				return new TimeSpan(Ticks);
			}
		}

		/// <summary>
		/// Gets the average time the timer was active per trigger.
		/// </summary>
		public TimeSpan AverageElapsed
		{
			get
			{
				return new TimeSpan(Ticks / count);
			}
		}
		/// <summary>
		/// Starts a new timed section.
		/// </summary>
		public TimedSection Start()
		{
			return new TimedSection(this);
		}

		void End(long elapsed)
		{
			Interlocked.Increment(ref count);
			Interlocked.Add(ref this.stopwatchTicks, elapsed);
		}

		/// <inheritdoc/>
		public override string ToString()
		{
			var count = this.count;
			var elapsed = this.TotalElasped;
			return $"Timer: {Name}, Count: {count}, Elapsed: {elapsed}, Average: {AverageElapsed}";
		}

		/// <summary>
		/// An timed section of a PerformanceTimer.
		/// </summary>
		public readonly struct TimedSection : IDisposable
		{
			readonly PerformanceTimer timer;
			readonly long startTicks;

			internal TimedSection(PerformanceTimer timer)
			{
				this.timer = timer;
				this.startTicks = Stopwatch.GetTimestamp();
			}

			internal void Stop()
			{
				var elapsed = Stopwatch.GetTimestamp() - startTicks;
				timer.End(elapsed);
			}

			/// <summary>
			/// Stops the timer.
			/// </summary>
			public void Dispose()
			{
				this.Stop();
			}
		}
	}
}
