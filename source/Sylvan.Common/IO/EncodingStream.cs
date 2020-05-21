using System;
using System.Diagnostics;
using System.Diagnostics.SymbolStore;
using System.IO;
using System.Text;

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

	class Base64Encoder : Encoder
	{
		static readonly byte[] DefaultEncodeMap =
			Encoding.ASCII.GetBytes("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");

		const int DefaultLineLength = 76;
		const byte Pad = (byte)'=';
		int carry;
		byte carry0;
		byte carry1;

		readonly byte[] encodeMap;
		readonly int maxLineLength;
		int lineIdx;

		int GetOutputBufferLength(int inputLength)
		{
			var l = (inputLength + 1) * 4 / 3;
			if (maxLineLength > 0)
			{
				l = (l + 1) * (maxLineLength + 2) / maxLineLength;
			}
			return l + 2;
		}


		public Base64Encoder() : this(DefaultLineLength)
		{
		}

		internal Base64Encoder(int lineLength)
		{
			this.encodeMap = DefaultEncodeMap;
			this.maxLineLength = lineLength;
		}

		public override EncoderResult Encode(ReadOnlySpan<byte> src, Span<byte> dst, out int bytesConsumed, out int bytesWritten)
		{
			var outputLen = GetOutputBufferLength(src.Length);
			bool final = src.Length == 0;

			int srcOffset = 0;
			int dstOffset = 0;
			var count = src.Length;
			//var dataLen = (dst.Length - 2) / 4 * 3;
			//var availDst = dataLen - (dataLen / maxLineLength) * 2;
			byte b0, b1, b2;

			if (carry > 0) // if we've carried anything from the previous block
			{
				if (carry + count >= 3) // if we can create a full triplet
				{
					if (carry == 1)
					{
						b0 = carry0;
						b1 = src[srcOffset++];
						b2 = src[srcOffset++];
						count -= 2;
					}
					else
					{
						b0 = carry0;
						b1 = carry1;
						b2 = src[srcOffset++];
						count -= 1;
					}
					if (dst.Length < 6) // ensure we have enough room in the output
					{
						bytesConsumed = bytesWritten = 0;
						return EncoderResult.RequiresOutputSpace;
					}

					dst[dstOffset++] = encodeMap[b0 >> 2];
					dst[dstOffset++] = encodeMap[((b0 & 0x03) << 4) | (b1 >> 4)];
					dst[dstOffset++] = encodeMap[((b1 & 0x0F) << 2) | (b2 >> 6)];
					dst[dstOffset++] = encodeMap[b2 & 0x3F];

					lineIdx += 4;
					if (maxLineLength > 0 && lineIdx >= maxLineLength)
					{
						dst[dstOffset++] = (byte)'\r';
						dst[dstOffset++] = (byte)'\n';
						lineIdx = 0;
					}
				}
				else // if we don't have enough to make a full triplet
				{
					bytesConsumed = 0;
					if (carry == 1)
					{
						if (src.Length == 1)
						{
							carry1 = src[srcOffset++];
							bytesConsumed = 1;
							carry = 2;
							count -= 1;
						}
					}
					if (final)
					{
						if (carry == 1)
						{
							b0 = carry0;
							dst[dstOffset++] = encodeMap[b0 >> 2]; // top 6 bits
							dst[dstOffset++] = encodeMap[(b0 & 0x03) << 4]; // bottom 2 bits
							dst[dstOffset++] = Pad;
							dst[dstOffset++] = Pad;
						}
						else if (carry == 2)
						{
							b0 = carry0;
							b1 = carry1;
							dst[dstOffset++] = encodeMap[b0 >> 2]; // top 6 bits
							dst[dstOffset++] = encodeMap[((b0 & 0x03) << 4) | (b1 >> 4)]; // 2,4 bits
							dst[dstOffset++] = encodeMap[(b1 & 0x0F) << 2]; // last 4 bits
							dst[dstOffset++] = Pad;
						}
					}
					else
					{
						bytesWritten = 0;
						return EncoderResult.RequiresInput;
					}
				}
			}

			for (int i = 0; i < count - 2; i += 3)
			{
				if (dst.Length - dstOffset < 6)
				{
					// if we don't have enough room in the output buffer
					// for an quad and a newline
					bytesConsumed = srcOffset;
					bytesWritten = dstOffset;
					return EncoderResult.RequiresOutputSpace;
				}

				b0 = src[srcOffset++];
				b1 = src[srcOffset++];
				b2 = src[srcOffset++];
				dst[dstOffset++] = encodeMap[b0 >> 2];
				dst[dstOffset++] = encodeMap[((b0 & 0x03) << 4) | (b1 >> 4)];
				dst[dstOffset++] = encodeMap[((b1 & 0x0F) << 2) | (b2 >> 6)];
				dst[dstOffset++] = encodeMap[b2 & 0x3F];

				lineIdx += 4;
				if (maxLineLength > 0 && lineIdx >= maxLineLength)
				{
					dst[dstOffset++] = (byte)'\r';
					dst[dstOffset++] = (byte)'\n';
					lineIdx = 0;
				}
			}

			// Handle the tail of the data.  There are 0, 1, or 2 remaining bytes.
			// The tail characters are enough to get the decode algorithm to
			// recover the byte(s).
			carry = src.Length - srcOffset;

			switch (carry)
			{
				case 0:
					break;
				case 1:
					carry0 = src[srcOffset++];
					break;
				case 2:
					carry0 = src[srcOffset++];
					carry1 = src[srcOffset++];
					break;
				default:
					Debug.Fail("invalid remainder");
					break;
			}


			bytesWritten = dstOffset;
			bytesConsumed = srcOffset;

			return carry == 0 ? EncoderResult.Flush : EncoderResult.RequiresInput;
		}
	}

	class EncoderStream : Stream
	{
		readonly Stream stream;
		readonly Encoder encoder;
		byte[] buffer;
		int bufferIdx;

		public EncoderStream(Stream stream, Encoder encoder)
		{
			this.stream = stream;
			this.encoder = encoder;
			this.buffer = new byte[0x1000];
		}

		public override bool CanRead => false;

		public override bool CanSeek => false;

		public override bool CanWrite => true;

		public override long Length => throw new NotSupportedException();

		public override long Position
		{
			get => throw new NotSupportedException();
			set => throw new NotSupportedException();
		}

		public override void Flush()
		{
			this.stream.Write(this.buffer, 0, bufferIdx);
			bufferIdx = 0;
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			while (count > 0)
			{
				var src = buffer.AsSpan().Slice(offset, count);
				var dst = this.buffer.AsSpan().Slice(bufferIdx);
				int dstCount;
				int srcCount;
				var result = this.encoder.Encode(src, dst, out srcCount, out dstCount);

				offset += srcCount;
				count -= srcCount;
				this.bufferIdx += dstCount;

				if (result == EncoderResult.RequiresOutputSpace)
				{
					Flush();
				}
			}
		}

		public override void Close()
		{
			var dst = this.buffer.AsSpan().Slice(bufferIdx);
			int srcCount, dstCount;
			var result = this.encoder.Encode(ReadOnlySpan<byte>.Empty, dst, out srcCount, out dstCount);
			this.bufferIdx += dstCount;
			if (result == EncoderResult.RequiresOutputSpace)
			{
				Flush();
				result = this.encoder.Encode(ReadOnlySpan<byte>.Empty, dst, out srcCount, out dstCount);
				this.bufferIdx += dstCount;
				Debug.Assert(result == EncoderResult.Complete);
			}
			Flush();
		}
	}
}
