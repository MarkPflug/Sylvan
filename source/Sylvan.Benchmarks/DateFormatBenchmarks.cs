using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Sylvan.Benchmarks;

public class SimpleInProc : Attribute, IConfigSource {

	public IConfig Config { get; }

	public SimpleInProc()
	{
		Config = ManualConfig
			.Create(DefaultConfig.Instance)
			.AddJob(Job.InProcess.WithInvocationCount(100000).WithIterationCount(4))
			.AddLogger(new ConsoleLogger())
			.WithUnionRule(ConfigUnionRule.AlwaysUseLocal);

	}
}

[SimpleInProc]
public class DateFormatBenchmarks
{
	public DateFormatBenchmarks()
	{
		// ensure the current culture is en-US
		var culture = CultureInfo.GetCultureInfo("en-US");
		CultureInfo.CurrentCulture = culture;
	}

	public IEnumerable<(DateTime,string)> Dates
	{
		get
		{
			yield return (new DateTime(2020, 10, 11, 12, 13, 14, DateTimeKind.Utc), "A");
			yield return (new DateTime(2020, 10, 11, 12, 13, 14, DateTimeKind.Unspecified), "B");
			yield return (new DateTime(2020, 10, 11, 12, 13, 14, DateTimeKind.Local), "C");
			yield return (new DateTime(2020, 10, 11, 0, 0, 0, DateTimeKind.Utc), "D");
			yield return (new DateTime(2020, 10, 11, 0, 0, 0, DateTimeKind.Utc).AddTicks(1234567), "E");
		}
	}

	[ParamsSource(nameof(Dates))]
	public (DateTime date, string kind) Date;

	[Benchmark]
	public string DateTimeToStringCurrent()
	{
		return Date.date.ToString();//en-US
	}

	[Benchmark]
	public string DateTimeToStringInvariant()
	{
		return Date.date.ToString(CultureInfo.InvariantCulture);
	}

	[Benchmark]
	public string DateTimeToStringOInvariant()
	{
		return Date.date.ToString("O", CultureInfo.InvariantCulture);
	}

	[Benchmark]
	public string DateTimeToStringFullExplicitInvariant()
	{
		return Date.date.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fffffffK", CultureInfo.InvariantCulture);
	}

	[Benchmark]
	public string DateTimeToStringDateOnlyExplicitInvariant()
	{
		return Date.date.ToString("yyyy'-'MM'-'dd", CultureInfo.InvariantCulture);
	}

	[Benchmark]
	public string InterpolatedStringComponents()
	{
		var d = Date.date;
		return $"{d.Year:0000}-{d.Month:00}-{d.Day:00}";
	}

	[Benchmark]
	public string ConcatStringComponents()
	{
		var d = Date.date;
		return d.Year.ToString("0000") + "-" + d.Month.ToString("00") + "-" + d.Day.ToString("00");
	}



	[Benchmark]
	public string IsoDateFormat()
	{
		return IsoDate.ToStringIso(Date.date);
	}
}
