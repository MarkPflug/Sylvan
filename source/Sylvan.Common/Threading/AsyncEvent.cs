using System.Threading.Tasks;

namespace Sylvan.Threading
{
	public sealed class AsyncEvent<T>
	{
		object sync;
		TaskCompletionSource<T> tcs;

		public AsyncEvent()
		{
			this.sync = new object();
			this.tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
		}

		public async Task<T> WaitAsync()
		{
			return await tcs.Task;
		}

		public void Set(T value)
		{
			lock (sync)
			{
				tcs.TrySetResult(value);
			}
		}

		public void Reset()
		{
			lock (sync)
			{
				if (tcs.Task.IsCompleted)
					tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
			}
		}
	}
}
