using System;
using System.Threading;

namespace Sylvan.Diagnostics
{
	public sealed class PerformanceCounter
	{
		public PerformanceCounter(string name)
		{
			this.Name = name;
			this.CreateTime = DateTime.UtcNow;
		}

		int count;

		public string Name { get; }

		public int Count => count;

		public DateTime CreateTime { get; }

		public double AverageFrequency
		{
			get
			{
				var duration = DateTime.UtcNow - CreateTime;
				return count / duration.TotalSeconds;
			}
		}

		public void Increment()
		{
			Interlocked.Increment(ref count);
		}

		public override string ToString()
		{
			return $"Counter: {Name}, Count: {Count}, AvgFreq: {AverageFrequency}";
		}
	}
}
