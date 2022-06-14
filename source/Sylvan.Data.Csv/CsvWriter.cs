using System;
using System.Data.Common;

namespace Sylvan.Data.Csv;

partial class CsvDataWriter
{
	sealed class WriterContext
	{
		internal CsvDataWriter writer;
		internal DbDataReader reader;

		public WriterContext(CsvDataWriter writer, DbDataReader reader)
		{
			this.writer = writer;
			this.reader = reader;
		}
	}

	abstract class CsvWriter
	{
		internal static readonly CsvWriter Escaped = new EscapedCsvWriter();
		internal static readonly CsvWriter Quoted = new QuotedCsvWriter();

#if SPAN
		public virtual int Write(WriterContext context, ReadOnlySpan<char> value, char[] buffer, int offset)
#else
		public virtual int Write(WriterContext context, string value, char[] buffer, int offset)
#endif
		{
			return WriteEscaped(context, value, buffer, offset);
		}

#if SPAN
		public abstract int WriteEscaped(WriterContext context, ReadOnlySpan<char> value, char[] buffer, int offset);
#else
		public abstract int WriteEscaped(WriterContext context, string value, char[] buffer, int offset);
#endif

	}

	sealed class EscapedCsvWriter : CsvWriter
	{
#if SPAN
		public override int WriteEscaped(WriterContext context, ReadOnlySpan<char> value, char[] buffer, int offset)
#else
		public override int WriteEscaped(WriterContext context, string value, char[] buffer, int offset)
#endif
		{
			// for simplicity assume every character will need to be escaped
			if (offset + value.Length * 2 >= buffer.Length)
				return InsufficientSpace;

			var p = offset;
			var needsEscape = context.writer.needsEscape;
			var escape = context.writer.escape;

			for (int i = 0; i < value.Length; i++)
			{
				char c = value[i];
				if (c >= needsEscape.Length || !needsEscape[c])
				{
					buffer[p++] = c;
				}
				else
				{
					if (c == '\r')
					{
						buffer[p++] = escape;
						buffer[p++] = c;

						if (i + 1 < value.Length && value[i + 1] == '\n')
						{
							buffer[p++] = '\n';
							i++;
						}
					}
					else
					{
						buffer[p++] = escape;
						buffer[p++] = c;
					}
				}
			}
			return p - offset;
		}
	}

	sealed class QuotedCsvWriter : CsvWriter
	{
#if SPAN
		public override int Write(WriterContext context, ReadOnlySpan<char> value, char[] buffer, int offset)
#else
		public override int Write(WriterContext context, string value, char[] buffer, int offset)
#endif
		{
			var r = WriteValueOptimistic(context, value, buffer, offset);
			if (r == NeedsQuoting)
			{
				return WriteEscaped(context, value, buffer, offset);
			}
			return r;
		}
#if SPAN
		static int WriteValueOptimistic(WriterContext context, ReadOnlySpan<char> value, char[] buffer, int offset)
#else
		static int WriteValueOptimistic(WriterContext context, string value, char[] buffer, int offset)
#endif
		{
			var pos = offset;
			var needsEscape = context.writer.needsEscape;

			if (pos + value.Length >= buffer.Length)
				return InsufficientSpace;

			for (int i = 0; i < value.Length; i++)
			{
				var c = value[i];
				if (c >= needsEscape.Length || !needsEscape[c])
				{
					buffer[pos + i] = c;
				}
				else
				{
					return NeedsQuoting;
				}
			}

			return value.Length;
		}

#if SPAN
		public override int WriteEscaped(WriterContext context, ReadOnlySpan<char> value, char[] buffer, int offset)
#else
		public override int WriteEscaped(WriterContext context, string value, char[] buffer, int offset)
#endif
		{
			var p = offset;
			// require at least room for the 2 quotes and 1 escape.
			if (offset + value.Length + 3 >= buffer.Length)
				return InsufficientSpace;

			var quote = context.writer.quote;
			var escape = context.writer.escape;

			buffer[p++] = quote; // range guarded by previous if
			for (int i = 0; i < value.Length; i++)
			{
				var c = value[i];

				if (c == quote || c == escape)
				{
					if (p == buffer.Length)
						return InsufficientSpace;
					buffer[p++] = escape;
				}

				if (p == buffer.Length)
					return InsufficientSpace;
				buffer[p++] = c;
			}
			if (p == buffer.Length)
				return InsufficientSpace;
			buffer[p++] = quote;

			return p - offset;
		}
	}
}
