using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Threading
{
	// https://thomaslevesque.com/2015/06/04/async-and-cancellation-support-for-wait-handles/
	static class WaitHandleExtensions
	{
		public static async Task<bool> WaitOneAsync(this WaitHandle handle, int millisecondsTimeout, CancellationToken cancellationToken)
		{
			RegisteredWaitHandle? registeredHandle = null;
			CancellationTokenRegistration tokenRegistration = default;
			try
			{
				var tcs = new TaskCompletionSource<bool>();

				registeredHandle =
					ThreadPool.RegisterWaitForSingleObject(
						handle,
						(state, timedOut) => ((TaskCompletionSource<bool>?)state)?.TrySetResult(!timedOut),
						tcs,
						millisecondsTimeout,
						true
					);

				tokenRegistration =
					cancellationToken.Register(
						state => ((TaskCompletionSource<bool>?)state)?.TrySetCanceled(),
						tcs
					);
				return await tcs.Task.ConfigureAwait(false);
			}
			finally
			{
				if (registeredHandle != null)
					registeredHandle.Unregister(null);
				tokenRegistration.Dispose();
			}
		}

		public static Task<bool> WaitOneAsync(this WaitHandle handle, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return handle.WaitOneAsync((int)timeout.TotalMilliseconds, cancellationToken);
		}

		public static Task<bool> WaitOneAsync(this WaitHandle handle, CancellationToken cancellationToken = default)
		{
			return handle.WaitOneAsync(Timeout.Infinite, cancellationToken);
		}
	}
}
