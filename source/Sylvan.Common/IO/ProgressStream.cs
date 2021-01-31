using System;
using System.IO;

namespace Sylvan.IO
{
	class ProgressStream : Stream
	{
		readonly Stream inner;
		readonly Action<double> callback;
		readonly long progressSize;
		long length;
		long progressAccumulator;

		public ProgressStream(Stream inner, Action<double> callback, double factor)
		{
			this.inner = inner;
			this.callback = callback;

			this.length = inner.Length;
			this.progressSize = (long)(length * factor);
		}

		public override bool CanRead => inner.CanRead;

		public override bool CanSeek => inner.CanSeek;

		public override bool CanWrite => inner.CanWrite;

		public override long Length => inner.Length;

		public override long Position { get => inner.Position; set => inner.Position = value; }

		public override void Flush()
		{
			inner.Flush();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			var l = inner.Read(buffer, offset, count);
			progressAccumulator += l;
			bool needsUpdate = false;
			while (progressSize > 0 && progressAccumulator > progressSize)
			{
				progressAccumulator -= progressSize;
				needsUpdate = true;
			}
			if (needsUpdate)
			{
				callback((double)inner.Position / inner.Length);
			}
			return l;
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			return inner.Seek(offset, origin);
		}

		public override void SetLength(long value)
		{
			inner.SetLength(value);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			inner.Write(buffer, offset, count);
		}
	}
}