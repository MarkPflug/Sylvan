using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.IO;

/// <summary>
/// A stream that encodes data written to it.
/// </summary>
public sealed class EncoderStream : Stream
{
	readonly bool ownsStream;
	readonly Stream stream;
	readonly Encoder encoder;
	readonly byte[] buffer;
	bool isClosed;
	int bufferIdx;

	/// <summary>
	/// Creates a new EncoderStream.
	/// </summary>
	/// <param name="stream">The underlying stream to write to.</param>
	/// <param name="encoder">The encoder to use to write to the stream.</param>
	public EncoderStream(Stream stream, Encoder encoder)
	{
		this.stream = stream;
		this.encoder = encoder;
		this.buffer = new byte[0x1000];
		this.isClosed = false;
		this.ownsStream = false;
	}

	/// <inheritdoc/>
	public override bool CanRead => false;

	/// <inheritdoc/>
	public override bool CanSeek => false;

	/// <inheritdoc/>
	public override bool CanWrite => true;

	/// <inheritdoc/>
	public override long Length => throw new NotSupportedException();

	/// <inheritdoc/>
	public override long Position
	{
		get => throw new NotSupportedException();
		set => throw new NotSupportedException();
	}

	/// <inheritdoc/>
	public override void Flush()
	{
		this.stream.Write(this.buffer, 0, bufferIdx);
		bufferIdx = 0;
	}

	/// <inheritdoc/>
	public override async Task FlushAsync(CancellationToken cancel)
	{
#if NETSTANDARD2_1 || NETCOREAPP3_0_OR_GREATER
		await this.stream.WriteAsync(this.buffer.AsMemory().Slice(0, bufferIdx), cancel).ConfigureAwait(false);
#else
		await this.stream.WriteAsync(this.buffer, 0, bufferIdx, cancel).ConfigureAwait(false);
#endif
		bufferIdx = 0;
	}

	/// <inheritdoc/>
	public override int Read(byte[] buffer, int offset, int count)
	{
		throw new NotSupportedException();
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

	EncoderResult Encode(byte[] buffer, ref int offset, ref int count)
	{
		var src = buffer.AsSpan().Slice(offset, count);
		var dst = this.buffer.AsSpan().Slice(bufferIdx);
		int dstCount;
		int srcCount;
		var result = this.encoder.Encode(src, dst, out srcCount, out dstCount);

		offset += srcCount;
		count -= srcCount;
		this.bufferIdx += dstCount;
		return result;
	}

	/// <inheritdoc/>
	public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
	{
		while (count > 0)
		{
			var result = Encode(buffer, ref offset, ref count);
			if (result == EncoderResult.RequiresOutputSpace)
			{
				await FlushAsync(cancellationToken).ConfigureAwait(false);
			}
		}
	}

	/// <inheritdoc/>
	public override void Write(byte[] buffer, int offset, int count)
	{
		while (count > 0)
		{
			var result = Encode(buffer, ref offset, ref count);
			if (result == EncoderResult.RequiresOutputSpace)
			{
				Flush();
			}
		}
	}

	/// <inheritdoc/>
	public override void Close()
	{
		if (isClosed == false)
		{
			var dst = this.buffer.AsSpan().Slice(bufferIdx);
			int dstCount;
			var result = this.encoder.Encode(ReadOnlySpan<byte>.Empty, dst, out _, out dstCount);
			this.bufferIdx += dstCount;
			if (result == EncoderResult.RequiresOutputSpace)
			{
				Flush();
				dst = this.buffer.AsSpan().Slice(bufferIdx);
				result = this.encoder.Encode(ReadOnlySpan<byte>.Empty, dst, out _, out dstCount);
				this.bufferIdx += dstCount;
				Debug.Assert(result == EncoderResult.Complete, "" + result);
			}
			Flush();
			this.isClosed = true;
		}
	}

	/// <inheritdoc/>
	protected override void Dispose(bool disposing)
	{
		base.Dispose(disposing);
		this.Close();
		if (this.ownsStream)
			this.stream.Dispose();
	}
}
