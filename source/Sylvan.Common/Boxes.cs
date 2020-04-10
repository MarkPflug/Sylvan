using System;

namespace Sylvan
{
	public static class Boxes
	{
		static readonly object TrueBox = true;
		static readonly object FalseBox = false;

		static readonly Lazy<object[]> Ints = new Lazy<object[]>(IntBoxFactory);

		// cache -1 through 0xf
		const int IntShift = -1;
		const int IntCount = 17;

		static object[] IntBoxFactory()
		{
			var ints = new object[IntCount];
			for (int i = 0; i < IntCount; i++)
			{
				ints[i] = i + IntShift;
			}
			return ints;
		}

		public static object Box(bool value)
		{
			return value ? TrueBox : FalseBox;
		}

		public static object Box(int value)
		{
			var idx = value - IntShift;
			if ((uint)idx < IntCount)
			{
				return Ints.Value[idx];
			}
			return (object)value;
		}
	}
}
