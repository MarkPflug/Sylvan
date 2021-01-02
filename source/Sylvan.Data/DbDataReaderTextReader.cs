using System;
using System.IO;

namespace Sylvan.Data
{
	partial class DataReaderAdapter
	{
		// supports DbDataReaderAdapter.GetTextReader
		sealed class DbDataReaderTextReader : TextReader
		{
			readonly DataReaderAdapter reader;
			readonly int ordinal;
			readonly char[] peekBuffer;
			long position;

			public DbDataReaderTextReader(DataReaderAdapter reader, int ordinal)
			{
				this.reader = reader;
				this.ordinal = ordinal;
				this.position = 0;
				this.peekBuffer = new char[1];
			}

			public override int Read(char[] buffer, int index, int count)
			{
				var c = reader.GetChars(ordinal, position, buffer, index, count);
				position += c;
				return (int)c;
			}

			public override int Peek()
			{
				throw new NotSupportedException();
				//var l = Read(peekBuffer!, 0, 1);
				//position -= l;
				//return l == 0 ? -1 : peekBuffer![0];
			}

			public override int Read()
			{
				var l = Read(peekBuffer!, 0, 1);
				return l == 0 ? -1 : peekBuffer![0];
			}
		}
	}
}