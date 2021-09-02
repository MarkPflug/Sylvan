using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Sylvan.Benchmarks
{
	[SimpleJob(1, 4, 10, 100_000)]
	public class DateBenchmarks
	{
		public IEnumerable<string> DateStrs
		{
			get
			{
				yield return "2020-10-11";
				yield return "2020-10-11T12:13:14Z";
				yield return "2020-10-11T12:13:14.1234567Z";
				yield return "2020-10-11T12:13:14.1234567-07:00";
			}
		}

		[ParamsSource(nameof(DateStrs))]
		public string DateStr;

		[Benchmark]
		public DateTime DateTimeParse()
		{
			return DateTime.Parse(DateStr, CultureInfo.InvariantCulture);
		}

		[Benchmark]
		public DateTime DateTimeParseExactO()
		{
			// this will fail to parse all but the long formats
			return DateTime.TryParseExact(DateStr, "O", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt)
				? dt
				: throw new Exception();
		}

		[Benchmark]
		public DateTime IsoDateTryParse()
		{
			return IsoDate.TryParse(DateStr, out var dt) 
				? dt
				: throw new Exception();
		}
	}
}
