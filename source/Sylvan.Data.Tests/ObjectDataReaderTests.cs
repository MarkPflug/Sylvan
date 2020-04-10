using System;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace Sylvan.Data
{
	public class ObjectDataReaderTests
    {
		

		[Fact]
        public void Test1()
        {
			var dr = TestData.GetTestData(5);
			int count = 0;
			while (dr.Read())
			{
				count++;
			}
			Assert.Equal(5, count);
        }
    }
}
