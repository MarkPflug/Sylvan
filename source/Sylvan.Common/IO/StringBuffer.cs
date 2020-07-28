using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Sylvan.IO
{	
	/// <summary>
	/// A memory-backed <see cref="TextWriter"/> implementation.
	/// </summary>
	sealed class StringBuffer : TextWriter
	{
		const int DefaultBlockShift = 12; // default to 4k blocks
		const int InitialBlockCount = 8;

		readonly ArrayPool<char> bufferPool;
		readonly int blockShift;
		readonly int blockSize;
		readonly int blockMask;
		readonly bool clearOnReturn;

		int length;
		int position;

		char[]?[] blocks;

		public override Encoding Encoding => Encoding.Unicode;

		/// <summary>
		/// Creates a BlockMemoryStream using the shared array pool.
		/// </summary>
		public StringBuffer() : this(ArrayPool<char>.Shared)
		{
		}

		/// <summary>
		/// Creates a PooledMemoryStream.
		/// </summary>
		/// <param name="bufferPool">The <see cref="ArrayPool{T}"/> to use.</param>
		/// <param name="blockShift">The size of the buffer to use expressed 1 &lt;&lt; blockShift. (Valid values 6 - 24)</param>
		/// <param name="clearOnReturn">A boolean indicating whether to clear the buffers after use.</param>

		public StringBuffer(ArrayPool<char> bufferPool, int blockShift = DefaultBlockShift, bool clearOnReturn = false)
		{
			this.bufferPool = bufferPool;
			this.blocks = new char[]?[InitialBlockCount];
			this.blockShift = blockShift;
			this.blockSize = 1 << blockShift;
			this.blockMask = blockSize - 1;
			this.clearOnReturn = clearOnReturn;
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
				throw new ArgumentOutOfRangeException(nameof(count));
			if (offset + count > buffer.Length)
				throw new ArgumentOutOfRangeException(nameof(offset));

			this.Write(((Span<char>)buffer).Slice(offset, count));
		}

#if NETSTANDARD2_1
		public override void Write(ReadOnlySpan<char> buffer)
#else
		public void Write(ReadOnlySpan<char> buffer)
#endif
		{
			var offset = 0;
			var count = buffer.Length;

			var shift = blockShift;

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
					curBlock = bufferPool.Rent(blockSize);
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
			var shift = buffer.blockShift;
			var size = buffer.blockSize;
			var mask = buffer.blockMask;

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

			var str = new string('\0', this.length);
			fixed(char* p = str)
			{
				Span<char> span = new Span<char>(p, this.length);

				var c = length >> blockShift;
				for (int i = 0; i < c; i++)
				{
					var block = this.blocks[i];
					if (block != null)
						block.CopyTo(span);
					span = span.Slice(blockSize);
				}

				var rem = length & blockMask;
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
					this.bufferPool.Return(block, this.clearOnReturn);
			}
		}
	}
}
