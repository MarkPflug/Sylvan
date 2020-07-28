using System;

namespace Sylvan
{
	/// <summary>
	/// Exposes shared boxes for common primitives.
	/// </summary>
	public static class Objects
	{
		static readonly object TrueBox = true;
		static readonly object FalseBox = false;

		static readonly Lazy<object[]> Ints = new Lazy<object[]>(IntBoxFactory);

		// cache -1 through 50
		const int IntMin = -1;
		const int IntMax = 50;
		const int IntCount = IntMax - IntMin + 1;

		static object[] IntBoxFactory()
		{
			var ints = new object[IntCount];
			for (int i = 0; i < IntCount; i++)
			{
				ints[i] = IntMin + i;
			}
			return ints;
		}

		/// <summary>
		/// Gets a shared box for a boolean value.
		/// </summary>
		public static object Box(bool value)
		{
			return value ? TrueBox : FalseBox;
		}

		/// <summary>
		/// Gets a shared box for an integer value within a cerain range, or produces a new boxed value.
		/// </summary>
		public static object Box(int value)
		{
			var idx = value - IntMin;
			if ((uint)idx < IntCount)
			{
				return Ints.Value[idx];
			}
			return value;
		}
	}
}
