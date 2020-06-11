using System;

namespace Sylvan.IO
{
	/// <summary>
	/// The results of a call to <see cref="Encoder.Encode"/>.
	/// </summary>
	public enum EncoderResult
	{
		/// <summary>
		/// Indicates that all input data was consumed and encoded to the output buffer.
		/// </summary>
		/// <remarks>
		/// This result doesn't necessarily indicate that the output is complete however.
		/// A subsequent call to Encode with an empty input buffer might be required to 
		/// finalize the output encoding.
		/// </remarks>
		Flush,

		/// <summary>
		/// Indicates that more input data is required to continue encoding.
		/// </summary>
		RequiresInput,

		/// <summary>
		/// Indates that the output buffer space was exhausted before the input was fully encoded.
		/// </summary>
		RequiresOutputSpace,

		/// <summary>
		/// Indicates that the entire input buffer was encoded to the output and the result can be considerd complete.
		/// </summary>
		Complete,
	}

	/// <summary>
	/// A data encoder.
	/// </summary>
	/// <remarks>
	/// Encoders can potentially be stateful, maintaining information between calls to <see cref="Encode"/>.
	/// </remarks>
	public abstract class Encoder
	{
		/// <summary>
		/// Encodes data from <paramref name="src"/> into <paramref name="dst"/>.
		/// </summary>
		/// <param name="src">The source data to encode. Pass <see cref="ReadOnlySpan{T}.Empty"/> to indicate the final block.</param>
		/// <param name="dst">The destination for the encoded data.</param>
		/// <param name="bytesConsumed">The number of bytes consumed from the source buffer.</param>
		/// <param name="bytesWritten">The number of bytes written to the destination buffer.</param>
		/// <returns>A value indicate the status of the encode operation.</returns>
		public abstract EncoderResult Encode(ReadOnlySpan<byte> src, Span<byte> dst, out int bytesConsumed, out int bytesWritten);
	}
}
