using System;
using System.Diagnostics;
using System.IO;
using Xunit;

namespace Sylvan.IO;

public class ProgressStreamTests
{
	[Fact]
	public void Empty()
	{
		var ms = new MemoryStream(Array.Empty<byte>());
		int cbCount = 0;
		Action<double> cb =
			p =>
			{
				cbCount++;
				Debug.Assert(p == 1.0);
			};
		var ps = new ProgressStream(ms, cb);
		var buf = new byte[0x100];
		var l = ps.Read(buf, 0, buf.Length);
		Assert.Equal(0, l);
		Assert.Equal(1, cbCount);
	}
}
