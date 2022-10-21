using System;
using System.Threading;

namespace Sylvan.Diagnostics;

/// <summary>
/// A named counter that tracks the frequency with which it is called.
/// </summary>
public sealed class PerformanceCounter
{
	/// <summary>
	/// Creates a new PerformanceCounter.
	/// </summary>
	public PerformanceCounter(string name)
	{
		this.Name = name;
		this.CreateTime = DateTime.UtcNow;
	}

	int count;

	/// <summary>
	/// Gets the Name.
	/// </summary>
	public string Name { get; }

	/// <summary>
	/// Gets the Count.
	/// </summary>
	public int Count => count;

	/// <summary>
	/// Gets the UTC timestamp when the counter was created.
	/// </summary>
	public DateTime CreateTime { get; }

	/// <summary>
	/// Gets the average frequency with which the counter is hit, in hertz.
	/// </summary>
	public double AverageFrequency
	{
		get
		{
			var duration = DateTime.UtcNow - CreateTime;
			return count / duration.TotalSeconds;
		}
	}

	/// <summary>
	/// Increments the counter.
	/// </summary>
	public void Increment()
	{
		Interlocked.Increment(ref count);
	}

	/// <inheritdoc/>
	public override string ToString()
	{
		return $"Counter: {Name}, Count: {Count}, AvgFreq: {AverageFrequency}";
	}
}
