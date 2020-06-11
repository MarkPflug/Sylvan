using System;

namespace Sylvan.IO
{
	public sealed class HexEncoder : Encoder
	{
		static readonly byte[] UC =
			new byte[] { (byte)'0', (byte)'1', (byte)'2', (byte)'3', (byte)'4', (byte)'5', (byte)'6', (byte)'7', (byte)'8', (byte)'9', (byte)'A', (byte)'B', (byte)'C', (byte)'D', (byte)'E', (byte)'F' };

		static readonly byte[] LC =
			new byte[] { (byte)'0', (byte)'1', (byte)'2', (byte)'3', (byte)'4', (byte)'5', (byte)'6', (byte)'7', (byte)'8', (byte)'9', (byte)'a', (byte)'b', (byte)'c', (byte)'d', (byte)'e', (byte)'f' };

		readonly byte[] map;

		public HexEncoder(bool upperCase = true)
		{
			this.map = upperCase ? UC : LC;
		}

		public override unsafe EncoderResult Encode(ReadOnlySpan<byte> src, Span<byte> dst, out int bytesConsumed, out int bytesWritten)
		{
			var len = Math.Min(src.Length, dst.Length / 2);

			fixed (byte* srcStart = src)
			fixed (byte* dstStart = dst)
			fixed (byte* mp = map)
			{
				var srcEnd = srcStart + len;
				var dstP = dstStart;
				for (var srcP = srcStart; srcP < srcEnd; srcP++)
				{
					*dstP++ = mp[*srcP >> 4];
					*dstP++ = mp[*srcP & 0xf];
				}
			}

			bytesConsumed = len;
			bytesWritten = len * 2;

			return len < src.Length
				? EncoderResult.RequiresOutputSpace
				: EncoderResult.Flush;
		}
	}
}
