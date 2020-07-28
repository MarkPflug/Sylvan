using System;
using System.IO;

namespace Sylvan.IO
{
	/// <summary>
	/// A readonly stream implementation that provides a random sequence of bytes.
	/// </summary>
	public sealed class RandomStream : Stream
	{
		readonly Random rand;
		byte[] temp;
		int bufferPos;
		long position;
		long length;

		/// <summary>
		/// Constructs a new RandomStream instance.
		/// </summary>
		public RandomStream(long length) : this(new Random(), length) { }

		/// <summary>
		/// Constructs a new RandomStream instance.
		/// </summary>
		public RandomStream(Random rand, long length)
		{
			if (rand == null) throw new ArgumentNullException(nameof(rand));
			this.rand = rand;
			this.temp = new byte[0x100];
			this.bufferPos = temp.Length;
			this.position = 0;
			this.length = length;
		}

		/// <inheritdoc/>
		public override bool CanRead => true;

		/// <inheritdoc/>
		public override bool CanSeek => false;

		/// <inheritdoc/>
		public override bool CanWrite => false;

		/// <inheritdoc/>
		public override long Length => length;

		/// <inheritdoc/>
		public override long Position { get => position; set => throw new NotSupportedException(); }

		/// <inheritdoc/>
		public override void Flush() { }

		/// <inheritdoc/>
		public override int Read(byte[] buffer, int offset, int count)
		{
			var c = 0;
			while (c < count && this.position < this.length)
			{
				if (bufferPos >= this.temp.Length)
				{
					rand.NextBytes(temp);
					bufferPos = 0;
				}
				var len = (int) Math.Min(Math.Min(count, temp.Length - bufferPos), this.length - this.position);
				Buffer.BlockCopy(this.temp, bufferPos, buffer, offset, len);
				offset += len;
				bufferPos += len;
				position += len;
				c += len;
			}
			return c;
		}

		/// <inheritdoc/>
		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		/// <inheritdoc/>
		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		/// <inheritdoc/>
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}
	}
}
