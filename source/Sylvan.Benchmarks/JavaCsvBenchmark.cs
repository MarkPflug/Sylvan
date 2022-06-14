//using BenchmarkDotNet.Attributes;
//using BenchmarkDotNet.Jobs;
//using Sylvan.Data.Csv;
//using System;
//using System.IO;

//namespace Sylvan.Benchmarks;

//// These are reproductions of benchmarks for the Java FastCsv library.
//// Benchmark from: https://github.com/osiegmar/JavaCsvBenchmarkSuite
//// Sylvan is comparatively slow at the Write benchmark. The reason for this is that
//// the benchmark data is both unrealistic: only strings and 4/5 need quoting
//// and the position of the delimiter/quote in most fields is a pathological case
//[SimpleJob(RuntimeMoniker.NetCoreApp31)]
//public class FastJavaCsvBenchmark
//{
//	const string Record = "Simple field,\"Example with separator ,\",\"Example with delimiter \"\"\",\"Example with\nnewline\",\"Example with , and \"\" and \nnewline\"\n";
//	CsvDataReader reader;
//	CsvWriter writer;
//	public FastJavaCsvBenchmark()
//	{
//		reader = CsvDataReader.Create(new InfiniteDataReader(Record));
//		writer = new CsvWriter(TextWriter.Null);
//	}

//	[Benchmark(Baseline = true)]
//	public void Write()
//	{
//		writer.WriteField("Simple field");
//		writer.WriteField("Example with separator ,");
//		writer.WriteField("Example with delimiter \"");
//		writer.WriteField("Example with\nnewline");
//		writer.WriteField("Example with , and \" and \nnewline");
//		writer.EndRecord();
//	}

//	[Benchmark]
//	public void Write2()
//	{
//		// even thought this writes an extra character per record over "Write",
//		// it almost 20% faster because it isn't worse case behavior
//		writer.WriteField("Simple field");
//		writer.WriteField("Example, with separator ");
//		writer.WriteField("\"Example\" with delimiter");
//		writer.WriteField("Example with\nnewline");
//		writer.WriteField("Example with , and \" and \nnewline");
//		writer.EndRecord();
//	}

//	[Benchmark]
//	public void Read()
//	{
//		reader.Read();
//	}
//}

//class InfiniteDataReader : TextReader
//{
//	private char[] data;
//	private int pos;

//	public InfiniteDataReader(string data)
//	{
//		this.data = data.ToCharArray();
//	}

//	public override int Read(char[] buffer, int index, int count)
//	{
//		int copied = 0;
//		while (copied < count)
//		{
//			int tlen = Math.Min(count - copied, data.Length - pos);
//			Array.Copy(data, pos, buffer, index + copied, tlen);
//			copied += tlen;
//			pos += tlen;

//			if (pos == data.Length)
//			{
//				pos = 0;
//			}
//		}

//		return copied;
//	}
//}
