using Sylvan.Data.Csv;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace Sylvan.Data
{
	public class ExampleBinderTests
	{
		[Fact]
		public void ExampleBinderTest1()
		{
			var csv = CsvDataReader.Create(new StringReader("Name,Count,Value\nTest,1,123.456"));
			var binder = new ExampleDynamicBinder(csv.GetColumnSchema());
			var result = new List<ExampleClass>();
			while (csv.Read())
			{
				var item = new ExampleClass();
				binder.Bind(csv, item);
				result.Add(item);
			}
		}

		[Fact]
		public void ExampleBinderTest2()
		{
			var csv = CsvDataReader.Create(new StringReader("Value,Name,Count\n123.456,Test,1"));
			var binder = new ExampleDynamicBinder(csv.GetColumnSchema());
			var result = new List<ExampleClass>();
			while (csv.Read())
			{
				var item = new ExampleClass();
				binder.Bind(csv, item);
				result.Add(item);
			}
		}

		[Fact]
		public void ExampleBinderTest3()
		{
			var csv = CsvDataReader.Create(new StringReader("Value,Name\n123.456,Test"));
			var binder = new ExampleDynamicBinder(csv.GetColumnSchema());
			var result = new List<ExampleClass>();
			while (csv.Read())
			{
				var item = new ExampleClass();
				binder.Bind(csv, item);
				result.Add(item);
			}
		}

		[Fact]
		public void BindTest()
		{
			var csv = CsvDataReader.Create(new StringReader("Value,Name\n123.456,Test"));
			var items = csv.Bind().ToArray();
		}
	}
}
