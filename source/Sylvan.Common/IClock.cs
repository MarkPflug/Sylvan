using System;

namespace Sylvan
{
	/// <summary>
	/// Provides an abstraction for getting the current time.
	/// </summary>
	public interface IClock
	{
		/// <summary>
		/// Gets the current time.
		/// </summary>
		DateTime UtcNow { get; }
	}

	/// <summary>
	/// An IClock implementation using the system clock.
	/// </summary>
	public sealed class SystemClock : IClock
	{
		private SystemClock() { }

		/// <summary>
		/// Gets the singleton instance.
		/// </summary>
		public readonly static IClock Instance = new SystemClock();

		
		DateTime IClock.UtcNow { get { return DateTime.UtcNow; } }
	}

	/// <summary>
	/// An IClock instance that allows manually advancing the time, for testing.
	/// </summary>
	public sealed class TestClock : IClock
	{
		/// <summary>
		/// Constructs a new TestClock.
		/// </summary>
		public TestClock(DateTime time)
		{
			this.time = time.Kind == DateTimeKind.Utc ? time : time.ToUniversalTime();
		}

		DateTime time;

		/// <summary>
		/// Gets the current time.
		/// </summary>
		public DateTime UtcNow => time;

		/// <summary>
		/// Advances the clock.
		/// </summary>
		public void Advance(TimeSpan duration)
		{
			if (duration < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));
			time += duration;
		}
	}
}
