using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Sylvan.IO
{
	/// <summary>
	/// A factory class for creating <see cref="PooledMemoryStream"/> instances.
	/// </summary>
	/// <remarks>
	/// This factory exists to allow tuning the parameters of the constructed <see cref="PooledMemoryStream"/>,
	/// while using a shared buffer pool.
	/// </remarks>
	sealed class StringBufferFactory : IFactory<StringBuffer>
	{
		const int DefaultBlockShift = 12; // this was determined to be the fastest.
		const int DefaultInitialBufferCount = 8;

		/// <summary>
		/// A default factory instance used by the default <see cref="PooledMemoryStream"/> constructor.
		/// </summary>
		public static readonly StringBufferFactory Default = new StringBufferFactory(new FixedArrayPool<char>(1 << DefaultBlockShift), DefaultBlockShift, DefaultInitialBufferCount);
				
		readonly ArrayPool<char> bufferPool;

		public StringBufferFactory(
			ArrayPool<char> bufferPool,
			int blockShift = DefaultBlockShift,
			int initialBufferCount = DefaultInitialBufferCount
		)
		{
			if (blockShift < 6 || blockShift > 16) //128 - 64k
				throw new ArgumentOutOfRangeException(nameof(blockShift));
			this.BlockShift = blockShift;
			this.BlockSize = 1 << blockShift;
			this.InitialBufferCount = initialBufferCount;
			this.bufferPool = bufferPool;
		}

        public int BlockShift { get; private set; }
        public int BlockSize { get; private set; }
        public int InitialBufferCount { get; private set; }

        public StringBuffer Create()
		{
			return new StringBuffer(this);
		}

		public void Return(char[] buffer)
		{
			bufferPool.Return(buffer);
		}

		public char[] Rent()
		{
			return bufferPool.Rent(BlockSize);
		}
	}

	/// <summary>
	/// A memory-backed <see cref="TextWriter"/> implementation.
	/// </summary>
	sealed class StringBuffer : TextWriter
	{
		readonly StringBufferFactory factory;

		int length;
		int position;

		char[]?[] blocks;

		public override Encoding Encoding => Encoding.Unicode;

		/// <summary>
		/// Creates a BlockMemoryStream using the default <see cref="BlockMemoryStreamFactory"/
		/// </summary>
		public StringBuffer() : this(StringBufferFactory.Default)
		{
		}

		public StringBuffer(StringBufferFactory factory)
		{
			this.factory = factory;
			this.blocks = new char[]?[factory.InitialBufferCount];
		}

		public override void Flush()
		{
		}

		public override void Write(string? value)
		{
			if (value == null) return;
			Write(value.AsSpan());
		}

		public override void Write(char[] buffer, int offset, int count)
		{
			if (count < 0)
				throw new ArgumentOutOfRangeException();
			if (offset + count > buffer.Length)
				throw new ArgumentOutOfRangeException();

			this.Write(((Span<char>)buffer).Slice(offset, count));
		}


		public void Write(ReadOnlySpan<char> buffer)
		{
			var offset = 0;
			var count = buffer.Length;

			var shift = factory.BlockShift;
			var blockMask = ~(~0 << shift);
			var blockSize = factory.BlockSize;

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

				var newBuffers = new char[]?[newBlockCount];
				Array.Copy(blocks, 0, newBuffers, 0, blocks.Length);
				this.blocks = newBuffers;
			}

			blocks = this.blocks;
			var pos = this.position;
			while (count > 0)
			{
				var blockIdx = pos >> shift;
				char[]? curBlock = blocks[blockIdx];
				if (curBlock == null)
				{
					curBlock = factory.Rent();
					blocks[blockIdx] = curBlock;
				}
				var blockOffset = (int)(pos & blockMask);
				var blockRem = blockSize - blockOffset;
				Debug.Assert(blockRem >= 0);
				var c = blockRem < count ? blockRem : count;
				Span<char> curSpan = curBlock;
				buffer.Slice(offset, c).CopyTo(curSpan.Slice(blockOffset));
				count -= c;
				pos = pos + c;
				offset += c;
			}
			this.position = (int)pos;
			if (this.position > this.length)
				this.length = this.position;
		}

		public override string ToString()
		{
			return BuildString();
		}

#if StringSpan

		string BuildString()
		{
			return String.Create(this.length, this, Writer);
		}

		static SpanAction<char, StringBuffer> Writer = StringBufferWriter;

		static void StringBufferWriter(Span<char> str, StringBuffer buffer)
		{
			var length = buffer.length;
			var shift = buffer.factory.BlockShift;
			var size = buffer.factory.BlockSize;
			var mask = ~(~0 << shift);

			var c = length >> shift;
			for (int i = 0; i < c; i++)
			{
				var block = buffer.blocks[i];
				if (block != null)
					block.CopyTo(str);
				str = str[size..];
			}

			var rem = length & mask;
			if (rem > 0)
			{
				ReadOnlySpan<char> block = buffer.blocks[c];
				block.Slice(0, rem).CopyTo(str);
			}
		}
#else
		unsafe string BuildString()
		{
			var length = this.length;
			var shift = this.factory.BlockShift;
			var size = this.factory.BlockSize;
			var mask = ~(~0 << shift);

			var str = new string('\0', this.length);
			fixed(char* p = str)
			{
				Span<char> span = new Span<char>(p, this.length);

				var c = length >> shift;
				for (int i = 0; i < c; i++)
				{
					var block = this.blocks[i];
					if (block != null)
						block.CopyTo(span);
					span = span.Slice(size);
				}

				var rem = length & mask;
				if (rem > 0)
				{
					ReadOnlySpan<char> block = this.blocks[c];
					block.Slice(0, rem).CopyTo(span);
				}
			}
			return str;
		}
#endif

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
