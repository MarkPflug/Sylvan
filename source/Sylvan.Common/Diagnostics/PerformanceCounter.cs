using System.Threading;

namespace Sylvan.Diagnostics
{
	public sealed class PerformanceCounter
	{
		public PerformanceCounter(string name)
		{
			this.Name = name;
		}

		int count;

		public string Name { get; }

		public int Count => count;

		public void Increment()
		{
			Interlocked.Increment(ref count);
		}

		public override string ToString()
		{
			var count = this.count;
			return $"Counter: {Name}, Count: {count}";
		}
	}
}
