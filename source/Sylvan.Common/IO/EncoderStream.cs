using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.IO
{
	public sealed class EncoderStream : Stream
	{
		readonly bool ownsStream;
		readonly Stream stream;
		readonly Encoder encoder;
		readonly byte[] buffer;
		bool isClosed;
		int bufferIdx;

		public EncoderStream(Stream stream, Encoder encoder)
		{
			this.stream = stream;
			this.encoder = encoder;
			this.buffer = new byte[0x1000];
			this.isClosed = false;
			this.ownsStream = false;
		}

		public override bool CanRead => false;

		public override bool CanSeek => false;

		public override bool CanWrite => true;

		public override long Length => throw new NotSupportedException();

		public override long Position
		{
			get => throw new NotSupportedException();
			set => throw new NotSupportedException();
		}

		public override void Flush()
		{
			this.stream.Write(this.buffer, 0, bufferIdx);
			bufferIdx = 0;
		}

		public override async Task FlushAsync(CancellationToken cancel)
		{
			await this.stream.WriteAsync(this.buffer, 0, bufferIdx, cancel).ConfigureAwait(false);
			bufferIdx = 0;
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

		EncoderResult Encode(byte[] buffer, ref int offset, ref int count)
		{
			var src = buffer.AsSpan().Slice(offset, count);
			var dst = this.buffer.AsSpan().Slice(bufferIdx);
			int dstCount;
			int srcCount;
			var result = this.encoder.Encode(src, dst, out srcCount, out dstCount);

			offset += srcCount;
			count -= srcCount;
			this.bufferIdx += dstCount;
			return result;
		}

		public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
		{
			while (count > 0)
			{
				var result = Encode(buffer, ref offset, ref count);
				if (result == EncoderResult.RequiresOutputSpace)
				{
					await FlushAsync(cancellationToken).ConfigureAwait(false);
				}
			}
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			while (count > 0)
			{
				var result = Encode(buffer, ref offset, ref count);
				if (result == EncoderResult.RequiresOutputSpace)
				{
					Flush();
				}
			}
		}

		public override void Close()
		{
			if (isClosed == false)
			{
				var dst = this.buffer.AsSpan().Slice(bufferIdx);
				int srcCount, dstCount;
				var result = this.encoder.Encode(ReadOnlySpan<byte>.Empty, dst, out srcCount, out dstCount);
				this.bufferIdx += dstCount;
				if (result == EncoderResult.RequiresOutputSpace)
				{
					Flush();
					result = this.encoder.Encode(ReadOnlySpan<byte>.Empty, dst, out srcCount, out dstCount);
					this.bufferIdx += dstCount;
					Debug.Assert(result == EncoderResult.Complete);
				}
				Flush();
				this.isClosed = true;
			}
		}

		protected override void Dispose(bool disposing)
		{
			this.Close();
			if (this.ownsStream)
				this.stream.Dispose();
		}
	}
}
