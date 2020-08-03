using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.IO
{
	/// <summary>
	/// A memory-backed <see cref="Stream"/> implementation using pooled buffers.
	/// </summary>
	/// <remarks>
	/// This class uses pooled buffers to reduce allocations, and memory clearing
	/// that are present with <see cref="MemoryStream"/>.
	/// </remarks>
	public sealed class PooledMemoryStream : Stream
	{
		const int DefaultBlockShift = 12; // default to 4k blocks
		const int InitialBlockCount = 8;
		
		readonly ArrayPool<byte> bufferPool;
		readonly int blockShift;
		readonly int blockSize;
		readonly int blockMask;
		readonly bool clearOnReturn;

		long length;
		long position;
		

		byte[]?[] blocks;

		/// <summary>
		/// Creates a PooledMemoryStream.
		/// </summary>
		public PooledMemoryStream() : this(ArrayPool<byte>.Shared, DefaultBlockShift, false)
		{
		}

		/// <summary>
		/// Creates a PooledMemoryStream.
		/// </summary>
		/// <param name="bufferPool">The <see cref="ArrayPool{T}"/> to use.</param>
		/// <param name="blockShift">The size of the buffer to use expressed 1 &lt;&lt; blockShift. (Valid values 6 - 24)</param>
		/// <param name="clearOnReturn">A boolean indicating whether to clear the buffers after use.</param>
		public PooledMemoryStream(ArrayPool<byte> bufferPool, int blockShift = DefaultBlockShift, bool clearOnReturn = false)
		{
			if (blockShift < 6 || blockShift > 24) // 64b - 16MB
				throw new ArgumentOutOfRangeException(nameof(blockShift));

			this.bufferPool = bufferPool;
			this.blocks = new byte[]?[InitialBlockCount];
			this.blockShift = blockShift;
			this.blockSize = 1 << blockShift;
			this.blockMask = blockSize - 1;
			this.clearOnReturn = clearOnReturn;
		}

		/// <inheritdoc/>
		public override bool CanRead => true;
		/// <inheritdoc/>
		public override bool CanSeek => true;
		/// <inheritdoc/>
		public override bool CanWrite => true;
		/// <inheritdoc/>
		public override long Length => length;
		/// <inheritdoc/>
		public override long Position
		{
			get
			{
				return position;
			}
			set
			{
				this.Seek(value, SeekOrigin.Begin);
			}
		}

		/// <inheritdoc/>
		public override void Flush()
		{
		}

		/// <inheritdoc/>
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (buffer == null) throw new ArgumentNullException(nameof(buffer));
			if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
			if (offset + count > buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset));

			var avail = this.length - this.position;
			var c = (int)(avail < count ? avail : count);
			var len = c;
			var pos = this.position;
			while (c > 0)
			{
				var blockIdx = pos >> blockShift;
				var curBlock = blocks[blockIdx];
				var blockOffset = (int)(pos & blockMask);
				var blockRem = blockSize - blockOffset;
				Debug.Assert(blockRem >= 0);
				var cl = blockRem < c ? blockRem : c;
				if (curBlock == null)
				{
					Array.Clear(buffer, offset, cl);
				}
				else
				{
					Buffer.BlockCopy(curBlock, blockOffset, buffer, offset, cl);
				}

				pos += cl;
				offset += cl;
				c -= cl;
			}

			this.position = pos;
			return len;
		}

		/// <inheritdoc/>
		public override long Seek(long offset, SeekOrigin origin)
		{
			long pos = 0;
			switch (origin)
			{
				case SeekOrigin.Begin:
					pos = offset;
					break;
				case SeekOrigin.Current:
					pos = this.position + offset;
					break;
				case SeekOrigin.End:
					pos = this.length + offset;
					break;
			}
			if (pos < 0 || pos > this.length)
				throw new ArgumentOutOfRangeException(nameof(offset));
			this.position = pos;
			return pos;
		}

		/// <inheritdoc/>
		public override void SetLength(long value)
		{
			if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));

			if (value < this.length)
			{
				long blocks = length >> blockShift;
				long newBlocks = value >> blockShift;

				// if the stream shrunk, return any unused blocks
				for (long i = newBlocks; i <= blocks && i < this.blocks.Length; i++)
				{
					var buffer = this.blocks[i];
					if (buffer != null)
					{
						this.blocks[i] = null;
						this.bufferPool.Return(buffer, clearOnReturn);
					}
					this.length = value;
				}
			}

			this.length = value;
		}

		/// <inheritdoc/>
		public override void Write(byte[] buffer, int offset, int count)
		{
			if (buffer == null)
				throw new ArgumentNullException(nameof(buffer));
			if (offset >= buffer.Length)
				throw new ArgumentOutOfRangeException(nameof(offset));
			if (count < 0 || offset + count > buffer.Length)
				throw new ArgumentOutOfRangeException(nameof(count));

			var shift = blockShift;
			var blockSize = this.blockSize;
			var blockMask = blockSize - 1;

			var endLength = this.position + count;
			var reqBlockCount = (endLength + blockMask) >> shift;

			var blocks = this.blocks;
			if (reqBlockCount > blocks.Length)
			{
				var newBlockCount = blocks.Length;
				while (newBlockCount < reqBlockCount)
				{
					newBlockCount <<= 1;
				}

				var newBuffers = new byte[]?[newBlockCount];
				Array.Copy(blocks, 0, newBuffers, 0, blocks.Length);
				this.blocks = newBuffers;
			}

			blocks = this.blocks;
			var pos = this.position;
			while (count > 0)
			{
				var blockIdx = pos >> shift;
				var curBlock = blocks[blockIdx];
				if (curBlock == null)
				{
					curBlock = bufferPool.Rent(this.blockSize);
					blocks[blockIdx] = curBlock;
				}
				var blockOffset = (int)(pos & blockMask);
				var blockRem = blockSize - blockOffset;
				Debug.Assert(blockRem >= 0);
				var c = blockRem < count ? blockRem : count;
				Buffer.BlockCopy(buffer, offset, curBlock, blockOffset, c);
				count -= c;
				pos = pos + c;
				offset += c;
			}
			this.position = pos;
			if (this.position > this.length)
				this.length = this.position;
		}

		/// <inheritdoc/>
		public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
		{
			if (destination == null) throw new ArgumentNullException(nameof(destination));

			while (position < length)
			{
				var rem = length - position;
				cancellationToken.ThrowIfCancellationRequested();
				var blockIdx = position >> blockShift;
				var block = this.blocks[blockIdx];
				var blockOffset = (int)(position & blockMask);
				var blockCount = blockSize - blockOffset;
				var blockLen = rem < blockCount ? (int)rem : blockCount;
				await destination.WriteAsync(block, blockOffset, blockLen).ConfigureAwait(false);
				position += blockLen;
			}
		}

		/// <inheritdoc/>
		protected override void Dispose(bool disposing)
		{
			foreach (var block in this.blocks)
			{
				if (block != null)
					this.bufferPool.Return(block, clearOnReturn);
			}
		}
	}
}
