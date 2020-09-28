using System;

namespace Sylvan.Data
{
	static class Extensions
	{
		public static T Repeat<T>(this T obj, Func<T, int,T> a, int count)
		{
			var item = obj;

			for (int i = 0; i < count; i++) {
				item = a(item, i);
			}

			return item;
		}
	}
}
