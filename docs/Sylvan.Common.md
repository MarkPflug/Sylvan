# Sylvan.Common

This library contains a number of utility types.


### Sylvan.Diagnostics.PerformanceTimer

A non-allocating performance timer.

```C#

class MyClass {

	// create a registered timer that can be accessed
	// by name
	static PerformanceTimer MyFunctionTimer = RegisteredTimer.Create("MyClass.MyFunction");

	void MyFunction() {

		using var timer = MyFunctionTimer.Start();
	
		// do other stuff that might throw an exception

	}
}


```

### Sylvan.Diagnostics.PerformanceCounter

A non-allocating performance counter. Very similar to `PerformanceTimer` but doesn't track time.



### Sylvan.IO.BytePattern

Implements a Boyer-Moore search algorithm for byte streams. Allows efficiently finding specific sequences of bytes within a larger stream.

```
var buffer = new byte[] {...};
var pattern = new BytePattern(new byte[] {0x01,0x02,0x03,x04});
var idx = pattern.Search(buffer)
````

### Sylvan.IO.PooledMemoryStream

An in-memory `Stream` implementation similar to `System.IO.MemoryStream`, that uses pooled buffers internally to provide optimized behavior for some scenarios. When using MemoryStream in scenarios where the required capacity is not known ahead of time the internal buffer will be grown as needed by allocating a new buffer and copying data from the previous buffer. This can result in a significant amount of time zeroing the newly allocated buffer and copying. PooledMemoryStream avoids this by re-using fixed-sized buffers from a shared pool. This avoid needing to clear the buffers (zero-fill) between use, and copying to a new buffer when the capacity is exceeded.

One significant disadvantage over MemoryStream is that it is not possible to access the steam data as a single contiguous block of memory, because it isn't stored that way internally.

The ArrayPool<byte> that is used can have a big impact on the performance. The default `ArrayPool<byte>.Shared` doesn't maintain a huge number of buffers. Once the pool is exausted it will begin allocating (and zeroing) buffers as needed. If this happens the performance of the PooledMemoryStream will significantly degrade and can end up being slower than a MemoryStream. Therefor it is important to understand the usage pattern, and potentially provide a custom ArrayPool<byte> that will be more liberal with holding on to memory than the `Shared` instance.
