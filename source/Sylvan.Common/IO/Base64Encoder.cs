using System;
using System.Diagnostics;
using System.Text;

namespace Sylvan.IO
{
	public sealed class Base64Encoder : Encoder
	{
		static readonly byte[] DefaultEncodeMap =
			Encoding.ASCII.GetBytes("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");

		const int DefaultLineLength = 76;
		const byte Pad = (byte)'=';

		readonly byte[] encodeMap;
		readonly int maxLineLength;

		int carry;
		byte carry0;
		byte carry1;

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

		public override unsafe EncoderResult Encode(ReadOnlySpan<byte> src, Span<byte> dst, out int bytesConsumed, out int bytesWritten)
		{
			bool final = src.Length == 0;

			int srcOffset = 0;
			int dstOffset = 0;
			var count = src.Length;
			byte b0, b1, b2;

			if (carry > 0) // if we've carried anything from the previous block
			{
				if (dst.Length < 6) // ensure we have enough room in the output
				{
					bytesConsumed = bytesWritten = 0;
					return EncoderResult.RequiresOutputSpace;
				}

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

					dst[dstOffset++] = encodeMap[b0 >> 2];
					dst[dstOffset++] = encodeMap[((b0 & 0x03) << 4) | (b1 >> 4)];
					dst[dstOffset++] = encodeMap[((b1 & 0x0F) << 2) | (b2 >> 6)];
					dst[dstOffset++] = encodeMap[b2 & 0x3F];

					lineIdx += 4;
					if (lineIdx >= maxLineLength)
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
							dst[dstOffset++] = encodeMap[b0 >> 2];
							dst[dstOffset++] = encodeMap[(b0 & 0x03) << 4];
							dst[dstOffset++] = Pad;
							dst[dstOffset++] = Pad;
						}
						else if (carry == 2)
						{
							b0 = carry0;
							b1 = carry1;
							dst[dstOffset++] = encodeMap[b0 >> 2];
							dst[dstOffset++] = encodeMap[((b0 & 0x03) << 4) | (b1 >> 4)];
							dst[dstOffset++] = encodeMap[(b1 & 0x0F) << 2];
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

			fixed (byte* srcStart = src.Slice(srcOffset))
			fixed (byte* dstStart = dst.Slice(dstOffset))
			fixed (byte* emp = encodeMap)
			{
				var srcP = srcStart;
				var dstP = dstStart;
				var dstEnd = dstStart + dst.Length;

				var safeEnd = dstEnd - 6;

				for (int i = 0; i < count - 2; i += 3)
				{
					if (dstP >= safeEnd)
					{
						// if we don't have enough room in the output buffer
						// for a quad and a newline
						bytesConsumed = srcOffset + (int)(srcP - srcStart);
						bytesWritten = dstOffset + (int)(dstP - dstStart);
						return EncoderResult.RequiresOutputSpace;
					}

					b0 = *srcP++;
					b1 = *srcP++;
					b2 = *srcP++;

					*dstP++ = emp[b0 >> 2];
					*dstP++ = emp[((b0 & 0x03) << 4) | (b1 >> 4)];
					*dstP++ = emp[((b1 & 0x0F) << 2) | (b2 >> 6)];
					*dstP++ = emp[b2 & 0x3F];

					lineIdx += 4;
					if (lineIdx >= maxLineLength)
					{
						*dstP++ = (byte)'\r';
						*dstP++ = (byte)'\n';
						lineIdx = 0;
					}
				}
				srcOffset += (int)(srcP - srcStart);
				dstOffset += (int)(dstP - dstStart);
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
}
