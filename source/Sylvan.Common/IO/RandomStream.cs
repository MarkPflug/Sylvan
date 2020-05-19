using System;
using System.IO;

namespace Sylvan.IO
{
	public sealed class RandomStream : Stream
	{
		readonly Random rand;
		byte[] buffer;
		int bufferPos;
		long position;

		public RandomStream() : this(new Random()) {}

		public RandomStream(Random rand)
		{
			this.rand = rand;
			this.buffer = new byte[0x1000];
			this.bufferPos = 0;
			this.position = 0;
		}

		public override bool CanRead => true;

		public override bool CanSeek => false;

		public override bool CanWrite => false;

		public override long Length => throw new NotSupportedException();

		public override long Position { get => position; set => throw new NotSupportedException(); }

		public override void Flush()
		{
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			var c = count;
			
			while(count > 0)
			{
				if(bufferPos >= buffer.Length)
				{
					rand.NextBytes(buffer);
					bufferPos = 0;
				}
				var len = Math.Min(count, buffer.Length - bufferPos);
				Buffer.BlockCopy(this.buffer, bufferPos, buffer, offset, len);
				offset += len;
				count -= len;
			}
			return c;
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
			throw new NotSupportedException();
		}
	}
}
