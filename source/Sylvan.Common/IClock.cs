using System;

namespace Sylvan
{
	public interface IClock
	{
		DateTime UtcNow { get; }
	}

	public class SystemClock : IClock
	{
		private SystemClock() { }

		public readonly static SystemClock Instance = new SystemClock();

		public DateTime UtcNow { get { return DateTime.UtcNow; } }
	}

	public class TestClock : IClock
	{
		public TestClock(DateTime time)
		{
			this.time = time.Kind == DateTimeKind.Utc ? time : time.ToUniversalTime();
		}

		DateTime time;

		public DateTime UtcNow => time;

		public void Advance(TimeSpan duration)
		{
			if (duration < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));
			time += duration;
		}
	}
}
