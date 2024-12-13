using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Sylvan.Data.Csv;

public class SchemaAnalyzerTests
{
	[Fact]
	public void Test1()
	{
		var data = "Id,Name,Col1,Col2,Col3\r\n1,Test,1,2,3";
		var csv = CsvDataReader.Create(new StringReader(data));
		var opts = new SchemaAnalyzerOptions { DetectSeries = true };
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(3, schema.Count);
		Assert.Equal("Id", schema[0].ColumnName);
		Assert.Equal("Name", schema[1].ColumnName);
		Assert.Equal("Col*", schema[2].ColumnName);
		Assert.Equal(typeof(int), schema[2].DataType);
	}

	[Fact]
	public void Test2()
	{
		var data = "Id,Name,Col1,Col2,Col3\r\n1,Test,1,2,3.77";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions { DetectSeries = true };
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(3, schema.Count);
		Assert.Equal("Id", schema[0].ColumnName);
		Assert.Equal("Name", schema[1].ColumnName);
		Assert.Equal("Col*", schema[2].ColumnName);
		Assert.Equal(typeof(double), schema[2].DataType);
	}

	/// <summary>
	/// Default SchemaAnalyzerOptions usage.
	/// </summary>
	[Fact]
	public void Test3_Default() //(Original behaviour before adding assessments for Timespan, DateOnly and TimeOnly types)
	{
		var data = "TimeSpan,DateTime,DateOnly,TimeOnly,DateTimeIsoFormat,Int\r\n48:12:23,20/12/2024 08:12:23,20/12/2024,08:12:23,2016-12-23T08:57:21.6490000,12345678";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions();
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(6, schema.Count);
		Assert.Equal(typeof(string), schema[0].DataType);   // 48:12:23
		Assert.Equal(typeof(DateTime), schema[1].DataType); // 20/12/2024 08:12:23
		Assert.Equal(typeof(DateTime), schema[2].DataType); // 20/12/2024
		Assert.Equal(typeof(DateTime), schema[3].DataType); // 08:12:23
		Assert.Equal(typeof(DateTime), schema[4].DataType); // 2016-12-23T08:57:21.6490000
		Assert.Equal(typeof(int), schema[5].DataType);      // 12345678
	}

	/// <summary>
	/// SchemaAnalyzerOptions DateTimeAndString. - Same as default behaviour
	/// </summary>
	[Fact]
	public void Test3a_DateTimeAndString()
	{
		var data = "TimeSpan,DateTime,DateOnly,TimeOnly,DateTimeIsoFormat,Int\r\n48:12:23,20/12/2024 08:12:23,20/12/2024,08:12:23,2016-12-23T08:57:21.6490000,12345678";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions()
		{
			DateOnlyTimeOnlyTimespanUsage = DateTimeTimespanDateOnlyTimeOnlyUsageOptions.DateTimeAndString
		};
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(6, schema.Count);
		Assert.Equal(typeof(string), schema[0].DataType);   // 48:12:23
		Assert.Equal(typeof(DateTime), schema[1].DataType); // 20/12/2024 08:12:23
		Assert.Equal(typeof(DateTime), schema[2].DataType); // 20/12/2024
		Assert.Equal(typeof(DateTime), schema[3].DataType); // 08:12:23
		Assert.Equal(typeof(DateTime), schema[4].DataType); // 2016-12-23T08:57:21.6490000
		Assert.Equal(typeof(int), schema[5].DataType);      // 12345678
	}

	/// <summary>
	/// SchemaAnalyzerOptions TimespanAndDateTime.
	/// </summary>
	[Fact]
	public void Test3b_TimespanThenDateTime()
	{
		var data = "TimeSpan,DateTime,DateOnly,TimeOnly,DateTimeIsoFormat,Int\r\n48:12:23,20/12/2024 08:12:23,20/12/2024,08:12:23,2016-12-23T08:57:21.6490000,12345678";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions()
		{
			DateOnlyTimeOnlyTimespanUsage = DateTimeTimespanDateOnlyTimeOnlyUsageOptions.TimespanAndDateTime
		};
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(6, schema.Count);
		Assert.Equal(typeof(TimeSpan), schema[0].DataType); // 48:12:23
		Assert.Equal(typeof(DateTime), schema[1].DataType); // 20/12/2024 08:12:23
		Assert.Equal(typeof(DateTime), schema[2].DataType); // 20/12/2024
		Assert.Equal(typeof(TimeSpan), schema[3].DataType); // 08:12:23
		Assert.Equal(typeof(DateTime), schema[4].DataType); // 2016-12-23T08:57:21.6490000
		Assert.Equal(typeof(int), schema[5].DataType);      // 12345678
	}

#if NET6_0_OR_GREATER
	/// <summary>
	/// SchemaAnalyzerOptions DateOnlyAndTimeOnlyOverDateTimeAndTimespan.
	/// </summary>
	[Fact]
	public void Test3c_DateOnlyAndTimeOnlyOverDateTimeAndTimespan()
	{
		var data = "TimeSpan,DateTime,DateOnly,TimeOnly,DateTimeIsoFormat,Int\r\n48:12:23,20/12/2024 08:12:23,20/12/2024,08:12:23,2016-12-23T08:57:21.6490000,12345678";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions()
		{
			DateOnlyTimeOnlyTimespanUsage = DateTimeTimespanDateOnlyTimeOnlyUsageOptions.TimeOnlyAndDateOnlyOverTimespanAndDateTime
		};
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(6, schema.Count);
		Assert.Equal(typeof(TimeSpan), schema[0].DataType); // 48:12:23
		Assert.Equal(typeof(DateTime), schema[1].DataType); // 20/12/2024 08:12:23
		Assert.Equal(typeof(DateOnly), schema[2].DataType); // 20/12/2024
		Assert.Equal(typeof(TimeOnly), schema[3].DataType); // 08:12:23
		Assert.Equal(typeof(DateTime), schema[4].DataType); // 2016-12-23T08:57:21.6490000
		Assert.Equal(typeof(int), schema[5].DataType);      // 12345678
	}
#endif

	[Fact]
	public void Test4()
	{
		var data = "Id,ColumnYes,ColumnNo\r\n1,Yes,No\r\n2,y,n\r\n2,Y,N";
		var csv = CsvDataReader.Create(new StringReader(data));

		var a = new SchemaAnalyzer();
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(3, schema.Count);
		Assert.Equal(typeof(int), schema[0].DataType);
		Assert.Equal(typeof(bool), schema[1].DataType);
		Assert.Equal(typeof(bool), schema[2].DataType);
	}

	[Fact]
	public void Test5()
	{
		var data = "Id,ColumnYep,ColumnNope\r\n1,YEP,NOPE,\r\n2,yep,nope";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions() { TrueStrings = new List<string> { "Yep" }, FalseStrings = new List<string> { "Nope" } };
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(3, schema.Count);
		Assert.Equal(typeof(int), schema[0].DataType);
		Assert.Equal(typeof(bool), schema[1].DataType);
		Assert.Equal(typeof(bool), schema[2].DataType);
	}

	[Fact]
	public void Test6()
	{
		var data = "Id,ColumnYep,ColumnNope\r\n1,Yes,No";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions() { TrueStrings = null, FalseStrings = null };
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(3, schema.Count);
		Assert.Equal(typeof(int), schema[0].DataType);
		Assert.NotEqual(typeof(bool), schema[1].DataType);
		Assert.NotEqual(typeof(bool), schema[2].DataType);
	}

	[Fact]
	public void Test7()
	{
		var data = "Id,ColumnYep,ColumnNope\r\n1,Yes,No";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions() { TrueStrings = new List<string>(), FalseStrings = new List<string>() };
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(3, schema.Count);
		Assert.Equal(typeof(int), schema[0].DataType);
		Assert.NotEqual(typeof(bool), schema[1].DataType);
		Assert.NotEqual(typeof(bool), schema[2].DataType);
	}
}