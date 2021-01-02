using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Data.XBase
{
	partial class XBaseDataReader
	{
		class MemoStream : Stream
		{
			XBaseDataReader dr;
			int length;
			int position;

			// the offset in the memoStream to the start of the data
			int offset;
			int rowIdx;
			int memoIdx;

			bool initialized;

			// TODO: this could probably live on the datareader and avoid an extra alloc
			byte[] tempBuffer;

			public MemoStream(XBaseDataReader dr, int ordinal)
			{
				this.tempBuffer = new byte[8];
				this.dr = dr;
				this.rowIdx = dr.recordIdx;

				this.position = 0;
				// can't determine the length without a read
				this.length = -1;

				var col = dr.columns[ordinal];
				this.memoIdx = BitConverter.ToInt32(dr.recordBuffer, col.offset);
				this.initialized = false;
			}

			public override bool CanRead => true;

			public override bool CanSeek => false;

			public override bool CanWrite => false;

			public override long Length
			{
				get
				{
					return length >= 0 ? length : long.MaxValue;
				}
			}

			public override long Position { 
				get => position;
				set => this.Seek(value, SeekOrigin.Begin);
			}

			public override void Flush()
			{
				this.FlushAsync().GetAwaiter().GetResult();
			}

			public override Task FlushAsync(CancellationToken cancellationToken)
			{
				return this.InitializeAsync();
			}

			async Task InitializeAsync()
			{
				if (!initialized)
				{
					var memoOffset = this.dr.memoBlockSize * this.memoIdx;
					
					this.offset = memoOffset + 8;

					this.dr.memoStream!.Seek(memoOffset, SeekOrigin.Begin);
					var c = await this.dr.memoStream.ReadAsync(tempBuffer, 0, 8);
					if (c != 8) throw new InvalidDataException();

					int marker = IPAddress.HostToNetworkOrder(BitConverter.ToInt32(tempBuffer, 0));
					if (marker != 1)
					{
						throw new NotSupportedException();
					}
					this.length = IPAddress.HostToNetworkOrder(BitConverter.ToInt32(tempBuffer, 4));
					this.initialized = true;
				}
			}

			public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
			{
				if(!this.initialized)
				{
					await InitializeAsync();
				}

				var c = Math.Min(this.length - position, count);
				// TODO: this feels inefficient.
				dr.memoStream!.Seek(this.offset + this.position, SeekOrigin.Begin);
				var l = await dr.memoStream!.ReadAsync(buffer, offset, c);
				if(l != c)
				{
					throw new InvalidDataException();
				}
				this.position += l;
				return c;
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				// TODO:
				return ReadAsync(buffer, offset, count).GetAwaiter().GetResult();
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new NotSupportedException();
			}

			//public override long Seek(long offset, SeekOrigin origin)
			//{
			//	if (!CanSeek)
			//		throw new NotSupportedException();

			//	long p = this.position;

			//	switch (origin)
			//	{
			//		case SeekOrigin.Begin:
			//			p = offset;
			//			break;
			//		case SeekOrigin.Current:
			//			p += offset;
			//			break;
			//		case SeekOrigin.End:
			//			p = this.length + offset;
			//			break;
			//	}
			//	if(p < 0 || p > this.length)
			//	{
			//		throw new ArgumentOutOfRangeException(nameof(offset));
			//	}
			//	// safe cast as p is already validated against length
			//	this.position = (int) p;
			//	return position;
			//}

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
}