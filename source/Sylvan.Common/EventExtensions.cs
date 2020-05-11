using System;
using System.Collections.Generic;

namespace Sylvan
{
	public static class EventExtensions
	{
		/// <summary>
		/// Invokes a multicast delegate, ensureing that all registered handlers are invoked and aggregating any exceptions.
		/// </summary>
		/// <exception cref="AggregateException">If any registered handlers throw.</exception>
		public static void InvokeAll<T>(this EventHandler<T> evt, object sender, T args) where T : class
		{
			if (evt == null) throw new ArgumentNullException(nameof(evt));

			List<Exception>? es = null;
			foreach (EventHandler<T> item in evt.GetInvocationList())
			{
				try
				{
					item(sender, args);
				}
				catch (Exception e)
				{
					if (es == null) es = new List<Exception>();
					es.Add(e);
				}
			}
			if (es != null)
				throw new AggregateException(es);
		}
	}
}
