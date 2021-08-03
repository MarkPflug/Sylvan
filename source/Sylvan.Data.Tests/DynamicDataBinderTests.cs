using Sylvan.Data.Csv;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data
{
	public class DynamicDataBinderTests
	{
		public class Person
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public DateTime BirthDate { get; set; }

		}

		[Fact]
		public void Test1()
		{
			var b = ObjectBinder.Get<Person>();

			var csv = CsvDataReader.Create(new StringReader("FirstName,LastName\nTest,User"));
			var binder = b.Create(csv);
			while (csv.Read())
			{
				var p = new Person();
				p.FirstName = "asdf";
				p.LastName = "qwer";
				p.BirthDate = DateTime.Now;
				binder.Bind(csv, p);
			}
		}
	}
}
