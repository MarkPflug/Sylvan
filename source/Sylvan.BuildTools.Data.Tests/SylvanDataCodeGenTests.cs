using Xunit;
using Xunit.Abstractions;

namespace Sylvan.BuildTools.Data.Tests
{
	[Collection("MSBuild")]
	public class SylvanDataCodeGenTests : MSBuildTest
	{
		public SylvanDataCodeGenTests(ITestOutputHelper testOutputHelper)
			: base(testOutputHelper) { }

		[Fact]
		public void BuildTest()
		{
			var exepath = BuildProject("Data/SeriesTest/Project.csproj");
			var (exitcode, output) = GetOutput(exepath);
			Assert.Equal(0, exitcode);
		}
	}
}
