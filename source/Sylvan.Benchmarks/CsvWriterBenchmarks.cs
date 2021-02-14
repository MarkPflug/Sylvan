using BenchmarkDotNet.Attributes;
using System.IO;
using System.Threading.Tasks;

namespace Sylvan.Data.Csv
{
	[MemoryDiagnoser]
	public class CsvWriterBenchmarks
	{
		static readonly int ValueCount = TestData.DefaultDataValueCount;

		[Benchmark]
		public void NaiveBroken()
		{
			TextWriter tw = TextWriter.Null;

			var items = TestData.GetTestObjects();
			tw.Write("Id");
			tw.Write(',');
			tw.Write("Name");
			tw.Write(',');
			tw.Write("Date");
			tw.Write(',');
			tw.Write("IsActive");
			for (int i = 0; i < ValueCount; i++)
			{
				tw.Write(',');
				tw.Write("Value" + i);
			}
			tw.WriteLine();

			foreach (var item in items)
			{
				tw.Write(item.Id);
				tw.Write(',');
				tw.Write(item.Name);
				tw.Write(',');
				tw.Write(item.Date);
				tw.Write(',');
				tw.Write(item.IsActive);
				for (int i = 0; i < ValueCount; i++)
				{
					tw.Write(',');
					tw.Write(item.DataSet[i]);
				}
				tw.WriteLine();
			}
		}

		[Benchmark]
		public async Task SylvanDataAsync()
		{
			var tw = TextWriter.Null;
			var dr = TestData.GetTestDataReader();
			var csv = CsvDataWriter.Create(tw);
			await csv.WriteAsync(dr);
		}

		[Benchmark]
		public void SylvanDataSync()
		{
			var tw = TextWriter.Null;
			var dr = TestData.GetTestDataReader();
			var csv = CsvDataWriter.Create(tw);
			csv.Write(dr);
		}
	}
}