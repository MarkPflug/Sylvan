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
		/// Gets the default Base64Codec, which uses the standard Base64 alphabet.
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
		/// Creates a new Base64Codec.
		/// </summary>
		public Base64Encoding()
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

		/// <summary>
		/// Base64 encodes data.
		/// </summary>
		/// <param name="data">The input data.</param>
		/// <returns>A base64 encoded string.</returns>
		public string Encode(byte[] data)
		{
			var bufferSize = GetOutputBufferLength(data.Length);
			char[] chars = new char[bufferSize];
			int lineIdx = 0;
			var len = EncodeInternal(data, 0, chars, 0, data.Length, ref lineIdx);
			return new String(chars, 0, len);
		}

		/// <summary>
		/// Base64 encodes data.
		/// </summary>
		public void Encode(Stream iStream, TextWriter writer)
		{
			// this buffer length MUST be a multiple of three for
			// this to work properly
			const int BufferLength = 3 * 0x1000;
			byte[] iBuffer = new byte[BufferLength];
			char[] oBuffer = new char[GetOutputBufferLength(BufferLength)];
			int len;
			int lineIdx = 0;
			while ((len = iStream.Read(iBuffer, 0, BufferLength)) > 0)
			{
				int count = EncodeInternal(iBuffer, 0, oBuffer, 0, len, ref lineIdx);
				writer.Write(oBuffer, 0, count);
			}
		}

		public int Encode(byte[] src, int srcOffset, char[] dst, int dstOffset, int count)
		{
			if (src == null) throw new ArgumentNullException("src");
			if (dst == null) throw new ArgumentNullException("dst");
			if (count < 0) throw new ArgumentOutOfRangeException("count");
			if (srcOffset + count > src.Length) throw new ArgumentException("src", "The length of src is less than srcOffset plus count.");

			var outputLen = GetOutputBufferLength(count);

			if (dstOffset + outputLen > dst.Length) throw new ArgumentException("dst", "The length of dst is less than srcOffset plus count * 2.");
			int lineIdx = 0;
			return EncodeInternal(src, srcOffset, dst, dstOffset, count, ref lineIdx);
		}

		int EncodeInternal(byte[] src, int srcOffset, char[] dst, int dstOffset, int count, ref int lineIdx)
		{
			// callers of this need to verify that count % 3 == 0, 
			// unless it is the last block of data being written.

			int startOffset = dstOffset;

			for (int i = 0; i < count - 2; i += 3)
			{
				byte b0 = src[srcOffset++];
				byte b1 = src[srcOffset++];
				byte b2 = src[srcOffset++];
				dst[dstOffset++] = encodeMap[(b0 >> 2) & 0x3F]; // b0 top 6 bits
				dst[dstOffset++] = encodeMap[((b0 & 0x03) << 4) | ((b1 >> 4) & 0x0F)]; // b0 bottom 2, b1 top 4
				dst[dstOffset++] = encodeMap[((b1 & 0x0F) << 2) | ((b2 >> 6) & 0x03)]; // b1 bottom 4, b2 top 2
				dst[dstOffset++] = encodeMap[(b2 >> 0) & 0x3F]; // b2 bottom 6 bits

				lineIdx += 4;
				if (lineLength > 0 && lineIdx >= lineLength)
				{
					dst[dstOffset++] = '\r';
					dst[dstOffset++] = '\n';
					lineIdx = 0;
				}
			}

			// Handle the tail of the data.  There are 0, 1, or 2 remaining bytes.
			// The tail characters are enough to get the decode algorithm to
			// recover the byte(s).

			int rem = count % 3;
			if (rem == 1)
			{
				var b0 = src[srcOffset++];
				dst[dstOffset++] = encodeMap[(b0 >> 2) & 0x3F]; // top 6 bits
				dst[dstOffset++] = encodeMap[(b0 & 0x03) << 4]; // bottom 2 bits
				dst[dstOffset++] = '=';
				dst[dstOffset++] = '=';
			}
			else if (rem == 2)
			{
				var b0 = src[srcOffset++];
				var b1 = src[srcOffset++];
				dst[dstOffset++] = encodeMap[(b0 >> 2) & 0x3F]; // top 6 bits
				dst[dstOffset++] = encodeMap[((b0 & 0x03) << 4) | ((b1 >> 4) & 0x0F)]; // 2,4 bits
				dst[dstOffset++] = encodeMap[(b1 & 0x0F) << 2]; // last 4 bits
				dst[dstOffset++] = '=';
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

		int DecodeInternal(ReadOnlySpan<char> src, Span<byte> dst, int count, ref int accum, ref int bitCount)
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
