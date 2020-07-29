using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Sylvan.Collections
{
	/// <summary>
	/// Extension methods for IEnumerable.
	/// </summary>
	public static class EnumerableExtensions
	{
		/// <summary>
		/// Gets the minimum and maximum value in a sequence.
		/// </summary>
		public static (T min, T max) MinMax<T>(this IEnumerable<T> seq) where T : IComparable
		{
			if (seq == null) throw new ArgumentNullException(nameof(seq));

			T min, max;
			var e = seq.GetEnumerator();
			if (e.MoveNext())
			{
				min = max = e.Current;
			}
			else
			{
				return default;
			}

			while (e.MoveNext())
			{
				var cur = e.Current;
				min = cur.CompareTo(min) < 0 ? cur : min;
				max = cur.CompareTo(max) > 0 ? cur : max;
			}
			return (min, max);
		}

		/// <summary>
		/// Enumerates a heirarchy depth first.
		/// </summary>
		public static IEnumerable<T> RecurseDepthFirst<T>(this T item, Func<T, IEnumerable<T>> selector)
		{
			if (selector == null) throw new ArgumentNullException(nameof(selector));
			var seq = selector(item);
			return RecurseDepthFirst(seq, selector);
		}

		/// <summary>
		/// Enumerates a heirarchy depth first.
		/// </summary>
		public static IEnumerable<T> RecurseDepthFirst<T>(this IEnumerable<T> seq, Func<T, IEnumerable<T>> selector)
		{
			if (seq == null) throw new ArgumentNullException(nameof(seq));

			var enumStack = new Stack<IEnumerator<T>>();

			var e = seq.GetEnumerator();
			try
			{
				while (true)
				{
					if (e.MoveNext())
					{
						var cur = e.Current;
						yield return cur;
						enumStack.Push(e);
						e = selector(cur).GetEnumerator();
					}
					else
					{
						e.Dispose();
						if (enumStack.Count > 0)
							e = enumStack.Pop();
						else
							break;
					}
				}
			}
			finally
			{
				while (enumStack.Count > 0)
				{
					e = enumStack.Pop();
					e.Dispose();
				}
			}
		}



		/// <summary>
		/// Gets the maximum item in a set based on a comparable key.
		/// </summary>
		/// <remarks>
		/// This behaves similarly to OrderByDescending().FirstOrDefault(), except
		/// that it doesn't require doing a complete sort.
		/// </remarks>
		public static T? MaxBy<T, TK>(this IEnumerable<T> seq, Func<T, TK> keySelector)
			where T : class
			where TK : IComparable<TK>
		{
			if (seq == null) throw new ArgumentNullException(nameof(seq));
			if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

			return OneBy(seq, keySelector, (k1, k2) => k1.CompareTo(k2) > 0);
		}

		/// <summary>
		/// Gets the maximum item in a set based on a comparable key.
		/// </summary>
		/// <remarks>
		/// This behaves similarly to OrderBy().FirstOrDefault(), except
		/// that it doesn't require doing a complete sort.
		/// </remarks>
		public static T? MinBy<T, TK>(this IEnumerable<T> seq, Func<T, TK> keySelector)
			where T : class
			where TK : IComparable<TK>
		{
			if (seq == null) throw new ArgumentNullException(nameof(seq));
			if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

			return OneBy(seq, keySelector, (k1, k2) => k1.CompareTo(k2) < 0);
		}

		// provides the implementation for MinBy/MaxBy.
		static T? OneBy<T, TK>(this IEnumerable<T> seq, Func<T, TK> keySelector, Func<TK, TK, bool> comparator)
			where T : class
			where TK : IComparable<TK>
		{
			T? oneItem = default;
			TK oneKey;

			using (var enumerator = seq.GetEnumerator())
			{
				if (enumerator.MoveNext())
				{
					oneItem = enumerator.Current;
					oneKey = keySelector(oneItem);

					while (enumerator.MoveNext())
					{
						var item = enumerator.Current;
						var key = keySelector(item);
						if (comparator(key, oneKey))
						{
							oneKey = key;
							oneItem = item;
						}
					}
				}
			}
			return oneItem;
		}

		/// <summary>
		/// Enumerates the closure of a graph given a starting node <paramref name="root"/>.
		/// </summary>
		public static IEnumerable<T> Closure<T>(
			T root,
			Func<T, IEnumerable<T>> children
		)
		{
			var seen = new HashSet<T>();
			var stack = new Stack<T>();
			stack.Push(root);

			while (stack.Count != 0)
			{
				T item = stack.Pop();
				if (seen.Contains(item))
					continue;
				seen.Add(item);
				yield return item;
				foreach (var child in children(item))
					stack.Push(child);
			}
		}

		/// <summary>
		/// Provides a topological ordering of a sequence based on node dependency.
		/// </summary>
		public static IEnumerable<T> OrderByTopological<T, TK>(this IEnumerable<T> seq, Func<T, TK> keySelector, Func<T, IEnumerable<TK>> dependencyKeySelector)
			where TK : IEquatable<TK>
		{
			var found = new HashSet<TK>();
			var items = seq.ToList();

			var depth = 0;
			// were any returned at depth x?
			bool any = false;

			foreach (var item in items)
			{
				var key = keySelector(item);
				var deps = dependencyKeySelector(item);
				if (!deps.Any())
				{
					found.Add(key);
					any = true;
					yield return item;
				}
			}

			while (any)
			{
				depth++;
				any = false;
				foreach (var item in items)
				{
					var key = keySelector(item);
					if (found.Contains(key))
						continue;

					bool isSatisfied = true;
					foreach (var depKey in dependencyKeySelector(item))
					{
						if (!found.Contains(depKey))
						{
							isSatisfied = false;
							break;
						}
					}

					if (isSatisfied)
					{
						any = true;
						found.Add(key);
						yield return item;
					}
				}
			}

			// if we weren't able to yield any of the items
			var failures = items.Where(a => !found.Contains(keySelector(a)));
			if (failures.Any())
			{
				var sw = new StringWriter();
				sw.WriteLine("Failed to apply topological ordering: ");
				foreach (var item in failures)
				{
					sw.WriteLine(keySelector(item).ToString());
				}

				throw new ArgumentException(sw.ToString());
			}
		}
	}
}
