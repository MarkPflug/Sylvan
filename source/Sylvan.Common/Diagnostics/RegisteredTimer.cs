using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Sylvan.Diagnostics
{
	public static class RegisteredTimer
	{
		static readonly ConcurrentDictionary<string, PerformanceTimer> timers;

		static RegisteredTimer()
		{
			timers = new ConcurrentDictionary<string, PerformanceTimer>();
		}

		public static IEnumerable<PerformanceTimer> RegisteredTimers
		{
			get
			{
				return timers.Values;
			}
		}

		public static PerformanceTimer? Get(string name)
		{
			return timers.TryGetValue(name, out var timer) ? timer : null;
		}

		public static PerformanceTimer Create(string name)
		{
			return timers.GetOrAdd(name, n => new PerformanceTimer(n));
		}
	}
}
