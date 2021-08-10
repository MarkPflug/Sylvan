#if NET6_0_OR_GREATER

using System;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Xunit;

namespace Sylvan.Data.Csv
{
	public unsafe class SimdExperiment
	{
		[Fact]
		public void Test1()
		{
			var mask = VectorForChar('\n');

			var data = "012\n456\n".ToCharArray();
			Span<byte> buf = stackalloc byte[16] {
				255,0, 255, 0,
				255,0, 255, 0,
				255,0, 255, 0,
				255,0, 255, 0,
			};


			Span<byte> destBuf = stackalloc byte[16];

			fixed (char* p = data)
			fixed (byte* b = buf)
			fixed (byte* dest = destBuf)

			{

				Vector128<byte> vb = Sse2.LoadVector128(b);
				ushort* ip = (ushort*)p;
				var vec = Sse2.LoadVector128(ip);
				var comp = Sse2.CompareEqual(mask, vec);
				var bits = Sse2.MoveMask(comp.AsByte());
				while(bits != 0)
				{
					var idx = Bmi1.TrailingZeroCount((uint)bits);

					var mm = ~((1 << ((int)idx + 2)) - 1);
					bits &= mm;
				}
			}

		}

		static Vector128<ushort> VectorForChar(char c)
		{
			ushort b = (ushort)c;
			return Vector128.Create(
				b, b, b, b,
				b, b, b, b
			);
		}
	}
}
#endif