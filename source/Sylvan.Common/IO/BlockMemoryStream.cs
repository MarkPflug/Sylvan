using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.IO
{
	/// <summary>
	/// A factory class for creating <see cref="BlockMemoryStream"/> instances.
	/// </summary>
	/// <remarks>
	/// This factory exists to allow tuning the parameters of the constructed <see cref="BlockMemoryStream"/>,
	/// while using a shared buffer pool.
	/// </remarks>
	public sealed class BlockMemoryStreamFactory : IFactory<BlockMemoryStream>
	{
		const int DefaultBlockShift = 12; // use 4k blocks
		const int DefaultInitialBufferCount = 8;

		/// <summary>
		/// A default factory instance used by the default <see cref="BlockMemoryStream"/> constructor.
		/// </summary>
		public static readonly BlockMemoryStreamFactory Default = new BlockMemoryStreamFactory(DefaultBlockShift, DefaultInitialBufferCount);

		readonly ArrayPool<byte> bufferPool;

		public BlockMemoryStreamFactory(
			int blockShift = DefaultBlockShift,
			int initialBufferCount = DefaultInitialBufferCount
		)
		{
			if (blockShift < 6 || blockShift > 16) //128 - 64k
				throw new ArgumentOutOfRangeException(nameof(blockShift));
			this.BlockShift = blockShift;
			this.BlockSize = 1 << blockShift;
			this.InitialBufferCount = initialBufferCount;
			this.bufferPool = new FixedArrayPool<byte>(this.BlockSize);
		}

		public int InitialBufferCount { get; }
		public int BlockShift { get; }
		internal int BlockSize { get; }

		public BlockMemoryStream Create()
		{
			return new BlockMemoryStream(this);
		}

		internal void Return(byte[] buffer)
		{
			bufferPool.Return(buffer);
		}

		internal byte[] Rent()
		{
			return bufferPool.Rent(BlockSize);
		}
	}

	/// <summary>
	/// A memory-backed <see cref="Stream"/> implementation.
	/// </summary>
	/// <remarks>
	/// This class uses pooled buffers to reduce allocations, and memory clearing
	/// that are present with <see cref="MemoryStream"/>.
	/// </remarks>
	public sealed class BlockMemoryStream : Stream
	{
		readonly BlockMemoryStreamFactory factory;

		long length;
		long position;

		byte[]?[] blocks;

		/// <summary>
		/// Creates a BlockMemoryStream using the default <see cref="BlockMemoryStreamFactory"/
		/// </summary>
		public BlockMemoryStream() : this(BlockMemoryStreamFactory.Default)
		{
		}

		public BlockMemoryStream(BlockMemoryStreamFactory factory)
		{
			if (factory == null) throw new ArgumentNullException(nameof(factory));
			this.factory = factory;
			this.blocks = new byte[]?[factory.InitialBufferCount];
		}

		public override bool CanRead => true;

		public override bool CanSeek => true;

		public override bool CanWrite => true;

		public override long Length => length;

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

		public override void Flush()
		{
		}

		int BlockMask => this.factory.BlockSize - 1;

		public override int Read(byte[] buffer, int offset, int count)
		{
			if (buffer == null) throw new ArgumentNullException(nameof(buffer));
			if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
			if (offset + count > buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset));

			var shift = factory.BlockShift;
			var blockMask = BlockMask;
			var blockSize = factory.BlockSize;


			var avail = this.length - this.position;
			var c = (int)(avail < count ? avail : count);
			var len = c;
			var pos = this.position;
			while (c > 0)
			{
				var blockIdx = pos >> shift;
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

		public override void SetLength(long value)
		{
			if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));

			if (value < this.length)
			{
				long blocks = length >> factory.BlockShift;
				long newBlocks = value >> factory.BlockShift;

				// if the stream shrunk, return any unused blocks
				for (long i = newBlocks; i <= blocks && i < this.blocks.Length; i++)
				{
					var buffer = this.blocks[i];
					if (buffer != null)
					{
						this.blocks[i] = null;
						this.factory.Return(buffer);
					}
					this.length = value;
				}
			}

			this.length = value;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			if (buffer == null) 
				throw new ArgumentNullException(nameof(buffer));
			if (offset >= buffer.Length) 
				throw new ArgumentOutOfRangeException(nameof(offset));
			if (count < 0 || offset + count > buffer.Length) 
				throw new ArgumentOutOfRangeException(nameof(count));

			var shift = factory.BlockShift;
			var blockSize = factory.BlockSize;
			var blockMask = blockSize - 1;

			var endLength = this.position + count;
			var reqBlockCount = (endLength + (int)blockMask) >> shift;

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
					curBlock = factory.Rent();
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
			this.position = (long)pos;
			if (this.position > this.length)
				this.length = this.position;
		}

		public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
		{
			if (destination == null) throw new ArgumentNullException(nameof(destination));
			var shift = factory.BlockShift;
			var blockMask = BlockMask;
			var blockSize = factory.BlockSize;

			while (position < length)
			{
				var rem = length - position;
				cancellationToken.ThrowIfCancellationRequested();
				var blockIdx = position >> shift;
				var block = this.blocks[blockIdx];
				var blockOffset = (int)(position & blockMask);
				var blockCount = blockSize - blockOffset;
				var blockLen = rem < blockCount ? (int)rem : blockCount;
				await destination.WriteAsync(block, blockOffset, blockLen).ConfigureAwait(false);
				position += blockLen;
			}
		}

		protected override void Dispose(bool disposing)
		{
			foreach (var block in this.blocks)
			{
				if (block != null)
					factory.Return(block);
			}
		}
	}
}
