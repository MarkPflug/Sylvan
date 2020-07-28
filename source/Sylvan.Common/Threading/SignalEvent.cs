using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sylvan.Threading
{
	sealed class SignalEvent : IDisposable
	{
		// This is the main triggering event.
		readonly EventWaitHandle set;
		// This second event is used to guard waiters from re-entering the first event before it has been reset.
		readonly EventWaitHandle reset;

		readonly Task eventLoopTask;
		readonly CancellationTokenSource cts;

		TaskCompletionSource<bool>? tcs;

		public SignalEvent(string name)
		{
			this.set = new EventWaitHandle(false, EventResetMode.ManualReset, name + "_set");
			this.reset = new EventWaitHandle(false, EventResetMode.ManualReset, name + "_reset");
			cts = new CancellationTokenSource();
			this.eventLoopTask = EventLoop(cts.Token);
		}

		async Task EventLoop(CancellationToken cancel)
		{
			while (!cancel.IsCancellationRequested)
			{
				tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
				await DoWaitAsync(cancel).ConfigureAwait(false);
				if (!cancel.IsCancellationRequested)
				{
					FireRaisedEvent();
				}
			}
		}

		async Task DoWaitAsync(CancellationToken cancel)
		{
			await set.WaitOneAsync(cancel).ConfigureAwait(false);
			await reset.WaitOneAsync(cancel).ConfigureAwait(false);
		}

		public bool Wait(TimeSpan timeout)
		{
			return this.tcs!.Task.Wait(timeout);
		}

		public Task<bool> WaitAsync()
		{
			return this.tcs!.Task;
		}

		public void Set()
		{
			reset.Reset();
			set.Set();
			set.Reset();
			reset.Set();
		}

		public event EventHandler<EventArgs>? Raised;

		void FireRaisedEvent()
		{
			Raised?.Invoke(this, EventArgs.Empty);
			this.tcs!.SetResult(true);
		}

		#region IDisposable Support

		bool disposedValue = false;

		void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					this.cts.Cancel();
					this.cts.Dispose();
					this.set.Dispose();
					this.reset.Dispose();				
				}
				disposedValue = true;
			}
		}

		public void Dispose()
		{
			Dispose(true);
		}

		#endregion
	}
}
