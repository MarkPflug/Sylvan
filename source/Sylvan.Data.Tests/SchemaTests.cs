using Xunit;

namespace Sylvan.Data
{
	public class SchemaTests
	{
		[Fact]
		public void Test1()
		{
			var spec = "A,B*";
			var s = SchemaSerializer.Simple.Read(spec);
			Assert.NotNull(s);
			var result = s.ToString();
			Assert.Equal(spec, result);
		}

		[Fact]
		public void SeriesTest()
		{
			var spec = "State:string[2],County:string[32],{Date}>Issues*:int?";
			var s = SchemaSerializer.Simple.Read(spec);
			Assert.NotNull(s);
			var result = s.ToString();
			Assert.Equal(spec, result, true);
			var issuesCol = s.GetColumnSchema()[2];
			Assert.Equal(typeof(int), issuesCol.DataType);
			Assert.True(issuesCol.AllowDBNull);
		}
	}
}
