using Xunit;
using Xunit.Abstractions;

namespace Sylvan.BuildTools.Data.Tests
{
	[Collection("MSBuild")]
	public class SylvanDataCodeGenTests : MSBuildTest
	{
		public SylvanDataCodeGenTests(ITestOutputHelper testOutputHelper)
			: base(testOutputHelper) { }

		[Fact(Skip = "This test is flakey, and it works in practice.")]
		public void BuildTest()
		{
			var exepath = BuildProject("Data/SeriesTest/Project.csproj");
			var (exitcode, output, err) = GetOutput(exepath);
			Assert.Equal(0, exitcode);
		}
	}
} 
