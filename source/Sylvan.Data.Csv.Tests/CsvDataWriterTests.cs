using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Sylvan.Data.Csv
{
	public class CsvDataWriterTests
	{
		[Fact]
		public async Task Simple()
		{
			var dr = TestData.GetData();

			var sw = new StringWriter();
			var csv = new CsvDataWriter(sw);
			await csv.WriteAsync(dr);
		}

		[Fact]
		public async Task Schema()
		{
			var dr = TestData.GetTypedData();
			var sw = new StringWriter();
			var csv = new CsvDataWriter(sw);
			await csv.WriteAsync(dr);
		}

		[Fact]
		public async Task Binary()
		{
			var dr = TestData.GetBinaryData();
			var sw = new StringWriter();
			using var csv = new CsvDataWriter(sw);
			await csv.WriteAsync(dr);
			var str = sw.ToString();
		}

		class TypedObject
		{
			public int Id { get; set; }
		}


		[Fact]
		public void WriteTypes()
		{
			var data = TestData.GetTypedData();
			var tw = new StringWriter();
			using var csv = new CsvDataWriter(tw);
			csv.Write(data);
			var str = tw.ToString();
		}
	}
}
