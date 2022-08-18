using BenchmarkDotNet.Attributes;

namespace Sylvan.Benchmarks;

[MemoryDiagnoser]
public class NumberBenchmarks
{
	const int Count = 100000;

	[Benchmark]
	public void IntToString()
	{
		for (int i = 0; i < Count; i++)
		{
			var s = i.ToString();
		}
	}

	[Benchmark]
	public double IntToDoubleCast()
	{
		double d = 0d;
		for (int i = 0; i < Count; i++)
		{
			d += (double)i;
		}
		return d;
	}

	[Benchmark]
	public void DoubleToString()
	{
		for (int i = 0; i < Count; i++)
		{
			var s = ((double)i).ToString();
		}
	}

	[Benchmark]
	public void IntParse()
	{
		for (int i = 0; i < Count; i++)
		{
			var x = int.Parse("123456");
		}
	}

	[Benchmark]
	public void DoubleParse()
	{
		for (int i = 0; i < Count; i++)
		{
			var x = double.Parse("123.45");
		}
	}
}
