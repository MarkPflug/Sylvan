using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Xunit;

namespace Sylvan.Primitives
{
	public class PrimitiveTests
	{
		[Fact]
		public void PrimitiveSize()
		{
			Assert.Equal(16, Marshal.SizeOf<Primitive>());
		}

		[Fact]
		public void EqualsTest()
		{
			var p1 = new Primitive(false);
			var p2 = new Primitive(0);
			Assert.True(p1 == p2);
		}

#if NETSTANDARD21

		[Fact]
		public void IndexerTest()
		{
			var g = Guid.Parse("000102030405060708090a0b0c0d0e0f");
			var p = new Primitive(g);
			var a = p[0];

		}
#endif
	}

	public class TypedPrimitiveTests
	{
		[Fact]
		public void Size()
		{
			Assert.Equal(20, Marshal.SizeOf<TypedPrimitive>());

			var arr = new TypedPrimitive[16];
			ref var elem0 = ref arr[0];
			ref var elem1 = ref arr[1];
			var offset = Unsafe.ByteOffset(ref elem0, ref elem1);
			Assert.Equal(20, (int)offset);
		}

		[Fact]
		public void EqualsTest()
		{
			var p1 = new TypedPrimitive(false);
			var p2 = new TypedPrimitive(0);
			Assert.False(p1 == p2);
		}
	}
}
