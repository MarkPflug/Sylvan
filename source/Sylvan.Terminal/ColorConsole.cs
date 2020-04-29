using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace Sylvan.Terminal
{
	public enum Direction : byte
	{
		Up = 0,
		Down,
		Right,
		Left,
	}

	public enum ScrollDirection : byte
	{
		Up = 0,
		Down = 1,
	}

	public partial class ColorConsole : TextWriter
    {
		readonly TextWriter tw;
		char[] buffer;

		public ColorConsole() : this(Console.Out)
		{
		}

		public bool IsColorEnabled
		{
			get;
		}

		public ColorConsole(TextWriter tw)
		{
			this.tw = tw;
			this.buffer = new char[0x100];
			this.IsColorEnabled = TryEnableConsoleColor();
		}

		static bool TryEnableConsoleColor()
		{
			if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
			{
				return EnableVTProcessing();
			}
			// other platforms should support VT escape sequences normally, I think... ?
			return true;
		}

		public void SetForeground(byte r, byte g, byte b)
		{
			SetColor(true, r, g, b);
		}

		public void SetBackground(byte r, byte g, byte b)
		{
			SetColor(false, r, g, b);
		}

		const char Escape = '\x1b';

		public override Encoding Encoding => tw.Encoding;

		public void SetDefaults()
		{
			buffer[0] = Escape;
			buffer[1] = '[';
			buffer[2] = '0';
			buffer[3] = 'm';

			tw.Write(buffer, 0, 4);
		}

		public void SetCode(string str)
		{
			if (str == null || str.Length == 0 || str.Length > 2) return; // nope

			buffer[0] = Escape;
			buffer[1] = '[';

			for (int i = 0; i < str.Length; i++)
			{
				buffer[2 + i] = str[i];
			}
			buffer[2 + str.Length] = 'm';

			tw.Write(buffer, 0, 3 + str.Length);
		}

		public void CursorMove(Direction d)
		{
			buffer[0] = Escape;

			switch (d)
			{
				case Direction.Up:
					buffer[1] = 'A';
					break;
				case Direction.Down:
					buffer[1] = 'B';
					break;
				case Direction.Right:
					buffer[1] = 'C';
					break;
				case Direction.Left:
					buffer[1] = 'D';
					break;
			}
			tw.Write(buffer, 0, 2);
		}

		public void SetColor(bool foreground, byte r, byte g, byte b)
		{			
			buffer[0] = Escape;
			buffer[1] = '[';
			buffer[2] = foreground ? '3' : '4';
			buffer[3] = '8';
			buffer[4] = ';';
			buffer[5] = '2';
			buffer[6] = ';';

			var idx = 7;
			idx += WriteByte(buffer, idx, r);
			buffer[idx++] = ';';
			idx += WriteByte(buffer, idx, g);
			buffer[idx++] = ';';
			idx += WriteByte(buffer, idx, b);
			buffer[idx++] = 'm';
			tw.Write(buffer, 0, idx);
		}

		public void NewBuffer()
		{
			tw.Write(Escape);
			tw.Write("[?1049h");
		}

		public void MainBuffer()
		{
			tw.Write(Escape);
			tw.Write("[?1049l");
		}

		int WriteByte(char[] buffer, int pos, byte value)
		{
			int len = StringLength(value);
			int index = pos + len;
			do
			{
				byte div = (byte)(value / 10);
				byte digit = (byte)(value - (10 * div));

				buffer[--index] = (char)('0' + digit);
				value = div;
			} while (value != 0);

			return len;
		}

		static int StringLength(byte value)
		{
			if (value < 10)
			{
				return 1;
			}

			if (value < 100)
			{
				return 2;
			}

			return 3;
		}

		public override void Write(string value)
		{
			tw.Write(value);
		}

		public override void WriteLine()
		{
			tw.WriteLine();
		}
	}
}
