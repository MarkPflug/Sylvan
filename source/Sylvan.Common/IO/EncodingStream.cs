using System;
using System.Diagnostics;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Text;

namespace Sylvan.IO
{
	

	

	class EncoderStream : Stream
	{
		readonly Stream stream;
		readonly Encoder encoder;
		readonly byte[] buffer;
		int bufferIdx;

		public EncoderStream(Stream stream, Encoder encoder)
		{
			this.stream = stream;
			this.encoder = encoder;
			this.buffer = new byte[0x1000];
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
			while (count > 0)
			{
				var src = buffer.AsSpan().Slice(offset, count);
				var dst = this.buffer.AsSpan().Slice(bufferIdx);
				int dstCount;
				int srcCount;
				var result = this.encoder.Encode(src, dst, out srcCount, out dstCount);

				offset += srcCount;
				count -= srcCount;
				this.bufferIdx += dstCount;

				if (result == EncoderResult.RequiresOutputSpace)
				{
					Flush();
				}
			}
		}

		public override void Close()
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
		}
	}
}
