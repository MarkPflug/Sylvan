using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Sylvan.IO
{
	// boyer moore search
	public sealed class BytePattern
	{
		readonly int[] pos;
		readonly int[] neg;
		readonly byte[] pattern;
		readonly int len;

		public int Length => len;

		public BytePattern(byte[] pattern)
		{
			if (pattern == null) throw new ArgumentNullException(nameof(pattern));

			this.pattern = pattern;
			this.len = pattern.Length;
			var last = len - 1;

			pos = new int[len];

			int idx = last;
			byte b = pattern[idx];
			pos[idx] = 1;
			idx--;
			int match;

			while (true)
			{
				while (true)
				{
					if (idx == -1)
						goto NegLoop;
					if (pattern[idx] == b)
						break;
					idx--;
				}

				match = last;
				var idx2 = idx;

				while (true)
				{
					if (idx2 == -1 || pattern[match] != pattern[idx2])
					{
						if (pos[match] == 0)
							pos[match] = match - idx2;

						break;
					}
					idx2--;
					match--;
				}
				idx--;
			}

		NegLoop:

			match = last - 1;
			while (match != -1)
			{
				if (pos[match] == 0)
					pos[match] = 1;

				match--;
			}
			neg = new int[0x100];

			for (int i = 0; i < 0x100; i++)
				neg[i] = len;

			for (idx = last; idx != -1; idx--)
			{
				b = pattern[idx];
				if (neg[b] == len)
					neg[b] = last - idx;
			}
		}

		public IEnumerable<int> SearchAll(byte[] data)
		{
			return SearchAll(data, 0, data.Length);
		}

		public IEnumerable<int> SearchAll(byte[] data, int startIdx, int endIdx)
		{
			while(startIdx >= 0)
			{
				var idx = Search(data, startIdx, endIdx);
				if (idx < 0) yield break;
				yield return idx;
				startIdx = idx + 1;
			}
		}

		public int Search(byte[] data, int offset)
		{
			return Search(data, offset, data.Length);
		}

		public int Search(byte[] data, int offset, int endIdx)
		{
			if (data == null) throw new ArgumentNullException(nameof(data));
			if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
			if (endIdx > data.Length) throw new ArgumentOutOfRangeException(nameof(endIdx));

			int startIdx = len - 1;
			int idx = offset + startIdx;

			byte pb = pattern[startIdx];
			byte db;

			while (true)
			{
				if (idx >= endIdx)
					return -1;

				db = data[idx];

				if (db != pb)
				{
					idx += neg[db];
				}
				else
				{
					var idx2 = idx;
					var match = startIdx;

					while (true)
					{
						if (match == 0)
							return idx2;

						match--;
						idx2--;

						db = data[idx2];

						if (db != pattern[match])
						{
							idx += Math.Max(pos[match], match - startIdx + neg[db]);
							break;
						}
					}
				}
			}
		}
	}
}
