using System;
using System.IO;

namespace Sylvan.IO
{
	/// <summary>
	/// Provides readonly-only access to a sub-range of an existing stream.
	/// </summary>
	sealed class SubStream : Stream
	{
		readonly Stream inner;
		readonly long length;

		long position;

		public SubStream(Stream inner, long length)
		{
			this.inner = inner;
			this.length = length;
			this.position = 0;
		}

		public override bool CanRead => inner.CanRead;

		public override bool CanSeek => inner.CanSeek;

		public override bool CanWrite => false;

		public override long Length => this.length;

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
			inner.Flush();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			var len = (int)Math.Min(count, this.length - this.position);
			var l = inner.Read(buffer, offset, len);
			this.position += l;
			return l;
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			if (!this.CanSeek) throw new NotSupportedException();

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
			if (pos < 0 || pos > length)
			{
				throw new ArgumentOutOfRangeException(nameof(offset));
			}

			inner.Seek(this.position - pos, SeekOrigin.Current); // this might throw, fine.
			this.position = pos;
			return this.position;
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
