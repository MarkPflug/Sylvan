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

#if NET6_0_OR_GREATER
	[Fact]
	public void Test3()
	{
		var data = "DateTime,DateOnly,TimeOnly,DateTimeIsoFormat\r\n20/12/2024 08:12:23,20/12/2024,08:12:23,2016-12-23T08:57:21.6490000";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions();
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(4, schema.Count);
		Assert.Equal(typeof(DateTime), schema[0].DataType);
		Assert.Equal(typeof(DateOnly), schema[1].DataType);
		Assert.Equal(typeof(TimeOnly), schema[2].DataType);
		Assert.Equal(typeof(DateTime), schema[3].DataType);
	}
#else
	[Fact]
	public void Test3()
	{
		var data = "DateTime,DateOnly\r\n20/12/2024 08:12:23,20/12/2024";
		var csv = CsvDataReader.Create(new StringReader(data));

		var opts = new SchemaAnalyzerOptions { DetectSeries = true };
		var a = new SchemaAnalyzer(opts);
		var result = a.Analyze(csv);

		var schema = result.GetSchema().GetColumnSchema();
		Assert.Equal(2, schema.Count);
		Assert.Equal(typeof(DateTime), schema[0].DataType);
		Assert.Equal(typeof(DateTime), schema[1].DataType);
	}
#endif

	[Fact]
	public void Test4()
	{
		var data = "Id,ColumnYep,ColumnNope\r\n1,Yes,No\r\n2,y,n\r\n2,Y,N";
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
