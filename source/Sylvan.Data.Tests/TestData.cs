using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Sylvan.Data
{
	class TestData
	{

		sealed class TestClass
		{
			public TestClass(int i)
			{
				Id = i;
				Name = "Name" + i;
				Value = Math.PI * i;
				Date = DateTime.Today.AddDays(i);
			}

			public int Id { get; set; }
			public string Name { get; set; }
			public double Value { get; set; }
			public DateTime Date { get; set; }
		}

		public static DbDataReader GetTestData(int count = 10)
		{
			return
				Enumerable
				.Range(0, count)
				.Select(i => new TestClass(i))
				.AsDataReader();
		}
	}
}
