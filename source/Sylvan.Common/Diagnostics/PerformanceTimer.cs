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
		// this default value is an overestimate.
		// on my dev machine BDN measures it at about 3.1e-8
		static double Overhead = 5e-8;

		// Calculates the cost of start/stop on the timer.
		// this seems useful because other platforms might be
		// radically different than the one I used to benchmark.
		public static double CalculateOverhead()
		{
			var timer = new PerformanceTimer("");
			// call once to JIT
			using (timer.Start()) { }

			// 1000 iterations was found to provide an estimate
			// that was pretty close to what BDN measures.
			const int Ops = 1000;

			var sw = Stopwatch.StartNew();
			for(int i = 0; i < Ops; i++)
			{
				using (timer.Start()) { }
			}
			sw.Stop();
			var overhead = sw.Elapsed.TotalSeconds / Ops;
			return overhead;
		}

		/// <summary>
		/// Constructs a new PerformanceTimer.
		/// </summary>
		public PerformanceTimer(string name)
		{
			this.Name = name;
			this.createTime = DateTime.UtcNow;
		}

		readonly DateTime createTime;
		long stopwatchTicks;
		int count;

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
		/// Gets the time when timer timer was created.
		/// </summary>
		public DateTime CreateTime => createTime;

		/// <summary>
		/// Gets the average frequency with which the timer has been triggered in hertz.
		/// </summary>
		public double AverageFrequency
		{
			get
			{
				var duration = DateTime.UtcNow - createTime;
				return count / duration.TotalSeconds;
			}
		}

		public double EstimatedOverhead
		{
			get
			{
				var oh = Overhead * Count;
				var frac = oh / (oh + TotalElasped.TotalSeconds);
				return frac;
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
			return $"Timer: {Name}, Count: {Count}, TotalElapsed: {TotalElasped}, AvgElapsed: {AverageElapsed} AvgFreq: {AverageFrequency}";
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

			/// <summary>
			/// Stops the timer.
			/// </summary>
			public void Dispose()
			{
				var elapsed = Stopwatch.GetTimestamp() - startTicks;
				timer.End(elapsed);
			}
		}
	}
}
