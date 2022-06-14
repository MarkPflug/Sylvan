using System;
using System.Data.Common;
using System.IO;

namespace Sylvan.Data;

/// <summary>
/// A Stream implementation over DbDataReader.GetBytes
/// </summary>
sealed class DbDataReaderStream : Stream
{
	readonly DbDataReader reader;
	readonly int ordinal;
	long position;

	public DbDataReaderStream(DbDataReader reader, int ordinal)
	{
		this.reader = reader;
		this.ordinal = ordinal;
		this.position = 0;
	}

	public override bool CanRead => true;

	public override bool CanSeek => false;

	public override bool CanWrite => false;

	public override long Length => throw new NotSupportedException();

	public override long Position
	{
		get => position;
		set => throw new NotSupportedException();
	}

	public override void Flush()
	{
		throw new NotSupportedException();
	}

	public override int Read(byte[] buffer, int offset, int count)
	{
		var len = reader.GetBytes(ordinal, position, buffer, offset, count);
		position += len;
		return (int)len; // why does GetBytes even return a long? weird.
	}

	public override long Seek(long offset, SeekOrigin origin)
	{
		throw new NotSupportedException();
	}

	public override void SetLength(long value)
	{
		throw new NotSupportedException();
	}

	public override void Write(byte[] buffer, int offset, int count)
	{
		throw new NotSupportedException();
	}
}
