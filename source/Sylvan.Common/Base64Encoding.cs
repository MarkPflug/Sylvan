using System;
using System.IO;

namespace Sylvan
{
	/// <summary>
	/// Provides encoding/decoding to Base64.
	/// </summary>
	public class Base64Encoding
	{
		static readonly char[] DefaultEncodeMap;
		static readonly byte[] DefaultDecodeMap;

		const int DecodeMapLength = 0x80;
		const int DefaultLineLength = 76;

		/// <summary>
		/// Gets the default Base64Encoding, which uses the standard Base64 alphabet.
		/// </summary>
		public static readonly Base64Encoding Default;

		static Base64Encoding()
		{
			DefaultEncodeMap = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".ToCharArray();
			DefaultDecodeMap = new byte[DecodeMapLength];
			InitializeDecodeMap(DefaultEncodeMap, DefaultDecodeMap);
			Default = new Base64Encoding();
		}

		static void InitializeDecodeMap(char[] encodeMap, byte[] decodeMap)
		{
			for (int i = 0; i < decodeMap.Length; i++)
				decodeMap[i] = 0xff;

			for (int i = 0; i < encodeMap.Length; i++)
			{
				var idx = encodeMap[i];
				if (idx >= decodeMap.Length)
					throw new ArgumentOutOfRangeException(nameof(encodeMap));
				decodeMap[idx] = (byte)i;
			}
		}

		int GetOutputBufferLength(int inputLength)
		{
			var l = (inputLength + 1) * 4 / 3;
			if (lineLength > 0)
			{
				l = (l + 1) * (lineLength + 2) / lineLength;
			}
			return l + 2;
		}

		readonly char[] encodeMap;
		readonly byte[] decodeMap;
		readonly int lineLength;

		/// <summary>
		/// Creates a new Base64Encoding.
		/// </summary>
		private Base64Encoding()
		{
			this.encodeMap = DefaultEncodeMap;
			this.decodeMap = DefaultDecodeMap;

			this.lineLength = DefaultLineLength;
		}

		internal Base64Encoding(int lineLength)
		{
			this.encodeMap = DefaultEncodeMap;
			this.decodeMap = DefaultDecodeMap;
			this.lineLength = lineLength;
		}
		
		public int Encode(byte[] src, int srcOffset, char[] dst, int dstOffset, int count)
		{
			if (src == null) throw new ArgumentNullException(nameof(src));
			if (dst == null) throw new ArgumentNullException(nameof(dst));
			if (count < 0 || srcOffset + count > src.Length) throw new ArgumentOutOfRangeException(nameof(count));

			var outputLen = GetOutputBufferLength(count);
			if (dstOffset + outputLen > dst.Length) throw new ArgumentOutOfRangeException(nameof(dstOffset));
		
			return EncodeInternal(src, srcOffset, dst, dstOffset, count);
		}

		unsafe int EncodeInternal(byte[] src, int srcOffset, char[] dst, int dstOffset, int count)
		{
			// callers of this need to verify that count % 3 == 0, 
			// unless it is the last block of data being written.
			var c = count - 2;
			int lineIdx = 0;
			int startOffset = dstOffset;
			fixed (byte* sp = &src[srcOffset])
			fixed (char* dp = &dst[dstOffset])
			fixed (char* ap = encodeMap)
			{
				var spp = sp;
				var dpp = dp;

				for (int i = 0; i < c; i += 3)
				{
					if (lineLength > 0 && lineIdx >= lineLength)
					{
						*dpp++ = '\r';
						*dpp++ = '\n';
						lineIdx = 0;
					}

					byte b0 = *spp++;
					byte b1 = *spp++;
					byte b2 = *spp++;
					*dpp++ = ap[(b0 >> 2)];
					*dpp++ = ap[(((b0 & 0x03) << 4) | (b1 >> 4))];
					*dpp++ = ap[(((b1 & 0x0F) << 2) | (b2 >> 6))];
					*dpp++ = ap[(b2 & 0x3F)];

					lineIdx += 4;
				}
				if (lineLength > 0 && lineIdx >= lineLength)
				{
					*dpp++ = '\r';
					*dpp++ = '\n';
					lineIdx = 0;
				}

				// Handle the tail of the data.  There are 0, 1, or 2 remaining bytes.
				// The tail characters are enough to get the decode algorithm to
				// recover the byte(s).

				int rem = count - (int)(spp - sp);
				if (rem == 1)
				{
					var b0 = *spp++;
					*dpp++ = *(ap + (b0 >> 2));
					*dpp++ = *(ap + ((b0 & 0x03) << 4));
					*dpp++ = '=';
					*dpp++ = '=';
				}
				else if (rem == 2)
				{
					var b0 = *spp++;
					var b1 = *spp++;
					*dpp++ = *(ap + (b0 >> 2));
					*dpp++ = *(ap + (((b0 & 0x03) << 4) | (b1 >> 4)));
					*dpp++ = *(ap + ((b1 & 0x0F) << 2));
					*dpp++ = '=';
				}
			}

			return dstOffset - startOffset;
		}				

		/// <summary>
		/// Decodes a base64 encoded string.
		/// </summary>
		/// <remarks>This method does not throw. Any invalid characters in the input stream are simply skipped.</remarks>
		/// <param name="src">Base64 encoded string.</param>
		/// <returns>The decoded data.</returns>
		public byte[] Decode(string src)
		{
			if (src == null) throw new ArgumentNullException(nameof(src));
			if (src.Length == 0) return Array.Empty<byte>();

			byte[] buffer = new byte[(src.Length * 3 + 3) / 4];

			// decoder state.
			int accum = 0;
			int bitCount = 0;

			int len = DecodeInternal(src.AsSpan(), buffer, src.Length, ref accum, ref bitCount);
			Array.Resize(ref buffer, len);
			return buffer;
		}

		/// <summary>
		/// Decodes base64 encoded data.
		/// </summary>
		/// <param name="src">Base64 encoded data.</param>
		/// <returns>The decoded data.</returns>
		public byte[] Decode(char[] src)
		{
			if (src == null) throw new ArgumentNullException(nameof(src));
			// decoder state.
			int accum = 0;
			int bitCount = 0;

			byte[] buffer = new byte[(src.Length * 3 + 3) / 4];
			int len = DecodeInternal(src, buffer, src.Length, ref accum, ref bitCount);
			Array.Resize(ref buffer, len);
			return buffer;
		}

		/// <summary>
		/// Decodes base64 encoded data.
		/// </summary>
		/// <param name="reader">The text reader containing base64 encoded data.</param>
		/// <param name="oStream">The stream to capture the decoded data.</param>
		public void Decode(TextReader reader, Stream oStream)
		{
			if (reader == null) throw new ArgumentNullException(nameof(reader));
			if (oStream == null) throw new ArgumentNullException(nameof(oStream));

			const int BufferLength = 0x1000;
			char[] iBuffer = new char[GetOutputBufferLength(BufferLength)];
			byte[] oBuffer = new byte[BufferLength];
			int accum = 0;
			int bitCount = 0;
			int len;
			while ((len = reader.Read(iBuffer, 0, BufferLength)) > 0)
			{
				int count = DecodeInternal(iBuffer, oBuffer.AsSpan(), len, ref accum, ref bitCount);
				oStream.Write(oBuffer, 0, count);
			}
		}

		unsafe int DecodeInternal(ReadOnlySpan<char> src, Span<byte> dst, int count, ref int accum, ref int bitCount)
		{
			int srcIdx = 0;
			int dstIdx = 0;
			for (int i = 0; i < count; i++)
			{
				char c = src[srcIdx++];
				if (c > DecodeMapLength) continue;

				if (c == '=') break;
				var b = decodeMap[c];

				if (b == 0xff) continue; //skip unmapped characters.

				accum <<= 6;
				accum |= b;
				bitCount += 6;
				if (bitCount >= 8)
				{
					bitCount -= 8;
					dst[dstIdx++] = (byte)((accum >> bitCount) & 0xff);
				}
			}
			return dstIdx;
		}
	}
}
