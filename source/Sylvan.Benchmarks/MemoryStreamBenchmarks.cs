using BenchmarkDotNet.Attributes;
using Sylvan.IO;
using System;
using System.Buffers;
using System.IO;

namespace Sylvan.Benchmarks
{
	[SimpleJob(1, 1, 8, 1)]
	[MemoryDiagnoser]
	public class MemoryStreamBenchmarks
	{
		[ParamsSource(nameof(LengthSource))]
		public int Length { get; set; }

		public static int[] LengthSource { get; } = new[] { 0x400, 0x100000, 0x1000000 }; //1k, 1mb, 16mb

		const int Iterations = 100;
		const int BufferSize = 0x10000;

		readonly byte[] data;

		static ArrayPool<byte> CustomPool;

		static MemoryStreamBenchmarks()
		{
			CustomPool = ArrayPool<byte>.Create(BufferSize * 2, 0x1000000 / (1<<12));
		}

		public MemoryStreamBenchmarks()
		{
			var data = new byte[BufferSize];
			var r = new Random();
			r.NextBytes(data);
			this.data = data;
		}

		[Benchmark(Baseline = true)]
		public void MemoryStream()
		{
			for (int i = 0; i < Iterations; i++)
			{
				// This is simulating that we don't know how big the data will be
				using var stream = new MemoryStream();
				Process(stream);
			}
		}

		[Benchmark]
		public void MemoryStreamPreAlloc()
		{
			for (int i = 0; i < Iterations; i++)
			{
				// benchmark performance when we know ahead of time
				using var stream = new MemoryStream(Length);
				Process(stream);
				if (stream.Capacity != Length)
					throw new InvalidOperationException();
			}
		}

		[Benchmark]
		public void PooledMemoryStreamSharedPool()
		{
			for (int i = 0; i < Iterations; i++)
			{
				using var stream = new PooledMemoryStream(ArrayPool<byte>.Shared);
				Process(stream);
			}
		}

		[Benchmark]
		public void PooledMemoryStreamCustomPool()
		{
			for (int i = 0; i < Iterations; i++)
			{
				using var stream = new PooledMemoryStream(CustomPool);
				Process(stream);
			}
		}

		void Fill(Stream stream)
		{
			int c = 0;
			var length = Length;
			while (c < length)
			{
				var l = Math.Min(data.Length, length - c);
				stream.Write(data, 0, l);
				c += l;
			}

			stream.Seek(0, SeekOrigin.Begin);
		}

		void Process(Stream stream)
		{
			Fill(stream);
			var dst = new CounterStream();
			stream.CopyTo(dst, BufferSize);
			if (dst.Length != Length)
				throw new InvalidOperationException();
		}

		class CounterStream : Stream
		{
			long c;
			public CounterStream()
			{
				this.c = 0;
			}

			public override bool CanRead => false;

			public override bool CanSeek => false;

			public override bool CanWrite => true;

			public override long Length => c;

			public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

			public override void Flush()
			{
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				throw new NotSupportedException();
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new NotSupportedException();
			}

			public override void SetLength(long value)
			{
				throw new NotSupportedException();
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				c += count;
			}
		}
	}
}
