using System;

namespace Sylvan.IO
{
	enum EncoderResult
	{
		Flush,
		RequiresInput,
		RequiresOutputSpace,
		Complete,
	}

	abstract class Encoder
	{
		public abstract EncoderResult Encode(ReadOnlySpan<byte> src, Span<byte> dst, out int bytesConsumed, out int bytesWritten);
	}
}
