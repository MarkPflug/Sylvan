using Xunit;

namespace Sylvan.Data;

public class SchemaBuilderTests
{
	[Fact]
	public void SchemaBuilderNullabilityTest()
	{
		var schema = 
			new Schema.Builder()
			.Add<int?>("Id")
			.Add<double?>("Value")
			.Add<long>("Count")
			.Build();

		Assert.True(schema[0].AllowDBNull);
		Assert.Equal(typeof(int), schema[0].DataType);

		Assert.True(schema[1].AllowDBNull);
		Assert.Equal(typeof(double), schema[1].DataType);

		Assert.False(schema[2].AllowDBNull);
		Assert.Equal(typeof(long), schema[2].DataType);
	}
}
