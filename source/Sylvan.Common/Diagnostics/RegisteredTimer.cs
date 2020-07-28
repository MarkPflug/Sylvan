using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Sylvan.Diagnostics
{
	/// <summary>
	/// A registry for named PerformanceTimers.
	/// </summary>
	public static class RegisteredTimer
	{
		static readonly ConcurrentDictionary<string, PerformanceTimer> timers;

		static RegisteredTimer()
		{
			timers = new ConcurrentDictionary<string, PerformanceTimer>();
		}

		/// <summary>
		/// Enumerates the registered performance timers.
		/// </summary>
		public static IEnumerable<PerformanceTimer> RegisteredTimers
		{
			get
			{
				return timers.Values;
			}
		}

		/// <summary>
		/// Finds a registered performance timer.
		/// </summary>
		/// <returns>The PerformanceTimer, or null of one wasn't found.</returns>
		public static PerformanceTimer? Find(string name)
		{
			return timers.TryGetValue(name, out var timer) ? timer : null;
		}

		/// <summary>
		/// Gets an existing, or creates a new registered timer.
		/// </summary>
		public static PerformanceTimer Create(string name)
		{
			return timers.GetOrAdd(name, n => new PerformanceTimer(n));
		}
	}
}
