using System;
using System.IO;

namespace Sylvan.IO
{
	public sealed class RandomStream : Stream
	{
		const int DefaultBufferSize = 0x100;
		readonly Random rand;
		byte[] temp;
		int bufferPos;
		long position;

		public RandomStream() : this(new Random(), DefaultBufferSize) { }

		public RandomStream(Random rand, int bufferSize)
		{
			if (rand == null) throw new ArgumentNullException(nameof(rand));
			if (bufferSize < 1) throw new ArgumentOutOfRangeException(nameof(bufferSize));
			this.rand = rand;
			this.temp = new byte[bufferSize];
			this.bufferPos = temp.Length;
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
			while (count > 0)
			{
				if (bufferPos >= this.temp.Length)
				{
					rand.NextBytes(temp);
					bufferPos = 0;
				}
				var len = Math.Min(count, temp.Length - bufferPos);
				Buffer.BlockCopy(this.temp, bufferPos, buffer, offset, len);
				offset += len;
				count -= len;
				bufferPos += len;
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
