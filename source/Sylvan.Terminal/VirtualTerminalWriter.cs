using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Sylvan.Terminal
{
	public enum Mode
	{
		Application = 0,
		Numeric,
	}

	public enum Color
	{
		Black = 0,
		Red,
		Green,
		Yellow,
		Blue,
		Magenta,
		Cyan,
		White,
	}

	static class Extensions
	{
		public static Span<char> WriteValue(this Span<char> s, byte value)
		{
			var l = WriteByte(s, value);
			return s.Slice(l);
		}

		public static int WriteValue(this Span<char> s, int offset, byte value)
		{
			var l = WriteByte(s.Slice(offset), value);
			return offset + l;
		}

		public static int WriteValue(this char[] s, int offset, byte value)
		{
			var l = WriteByte(s, offset, value);
			return offset + l;
		}

		static byte WriteByte(char[] buffer, int offset, byte value)
		{
			var len = StringLength(value);
			int index = len;

			do
			{
				byte div = (byte)(value / 10);
				byte digit = (byte)(value - (10 * div));

				buffer[offset + --index] = (char)('0' + digit);
				value = div;
			} while (value != 0);

			return len;
		}

		static byte WriteByte(Span<char> buffer, byte value)
		{
			var len = StringLength(value);
			int index = len;


			do
			{
				byte div = (byte)(value / 10);
				byte digit = (byte)(value - (10 * div));

				buffer[--index] = (char)('0' + digit);
				value = div;
			} while (value != 0);

			return len;
		}

		static byte StringLength(byte value)
		{
			return
				value >= 100
				? (byte)3
				: value >= 10
				? (byte)2
				: (byte)1;
		}
	}

	public class VirtualTerminalWriter  : TextWriter
	{
		const char Escape = '\x1b';
		const char Backspace = '\x7f';
		const char Pause = '\x1a';

		const char ValueStart = '[';
		const char ValueContinue = ';';
		const char Mode = '?';
		char[] buffer = new char[128];
		string newLine = Environment.NewLine;

		static class Code
		{
			public const char CursorUp = 'A';
			public const char CursorDown = 'B';
			public const char CursorRight = 'C';
			public const char CursorLeft = 'D';
			public const char CursorPosition = 'H';
			public const char Home = 'H';
			public const char End = 'F';

			public const char ReverseIndex = 'M';
			public const char CursorSave = '7';
			public const char CursorRestore = '8';

			public const char Enable = 'h';
			public const char Disable = 'l';
			public const char ScrollUp = 'S';
			public const char ScrollDown = 'T';
			public const char Insert = '@';
			public const char Delete = 'P';
			public const char Erase = 'X';
			public const char InsertLine = 'L';
			public const char DeleteLine = 'M';
			public const char Format = 'm';
		}

		static class Format
		{
			public const char Default = '0';
			public const char Bright = '1';
			public const char Underline = '4';
			public const char Negative = '7';

			public const char Toggle = '2';

			public const char Foreground = '3';
			public const char Background = '4';

			public const char Extended = '8';

			public const char BrightForeground = '9';
			public const string BrightBackground = "10";

		}

		int counter;
		readonly TextWriter output;

		public override Encoding Encoding => output.Encoding;

		public VirtualTerminalWriter(TextWriter output)
		{
			this.output = output;
		}

		public override void Write(char c)
		{
			output.Write(c);
		}

		public override void Write(char[] buffer, int offset, int length)
		{
			output.Write(buffer, offset, length);
		}

		public async Task FlushAsync()
		{
			await this.output.FlushAsync();
		}

		public void CursorMove(Direction d, byte c = 1)
		{
			WriteCode(GetCode(d), c);
		}

		public void WriteLine(String str)
		{
			Output(str);
			Output(this.newLine);
		}

		public void Write(string str)
		{
			Output(str);
		}

		public void SetCursorPosition(byte x, byte y)
		{
			Span<byte> args = stackalloc byte[] { y, x };
			WriteCode(Code.CursorPosition, args);
		}

		public void SetCursorBlink(bool enable)
		{
			var code =
				enable
				? Escape + "[?12" + Code.Enable
				: Escape + "[?12" + Code.Disable;
			Output(code);
		}

		public void SetCursorVisibility(bool visible)
		{
			var code =
				visible
				? Escape + "[?25" + Code.Enable
				: Escape + "[?25" + Code.Disable;

			Output(code);
		}

		public void EraseLine(int chars)
		{
			Output(Escape + "[" + chars + "K");
		}

		public void Scroll(ScrollDirection dir, byte count = 1)
		{
			WriteCode(GetCode(dir), count);
		}

		public void Insert(byte count = 1)
		{
			WriteCode(Code.Insert, count);
		}

		public void Delete(byte count = 1)
		{
			WriteCode(Code.Delete, count);
		}

		public void Erase(byte count = 1)
		{
			WriteCode(Code.Erase, count);
		}

		public void InsertLine(byte count = 1)
		{
			WriteCode(Code.InsertLine, count);
		}

		public void DeleteLine(byte count = 1)
		{
			WriteCode(Code.DeleteLine, count);
		}

		public void EraseInDisplay(byte count)
		{
			WriteCode('J', count);
		}

		public void EraseInLine(byte count)
		{
			WriteCode('K', count);
		}

		public void SetFormatDefault()
		{
			Output("" + Escape + ValueStart + "0" + Code.Format);
		}

		public void SetBright()
		{
			Output("" + Escape + ValueStart + "1" + Code.Format);
		}

		public void SetUnderline(bool on = true)
		{
			WriteCode(Code.Format, (byte)(on ? 4 : 24));
		}

		public void SetForgroundDefault()
		{
			WriteCode(Code.Format, 39);
		}

		public void SetBackgroundDefault()
		{
			WriteCode(Code.Format, 49);
		}

		public void SetForeground(Color c)
		{
			WriteCode(Code.Format, (byte)(30 + c));
		}

		public void SetBackground(Color c)
		{
			WriteCode(Code.Format, (byte)(40 + c));
		}

		public void SetForegroundBright(Color c)
		{
			WriteCode(Code.Format, (byte)(90 + c));
		}

		public void SetBackgroundBright(Color c)
		{
			WriteCode(Code.Format, (byte)(100 + c));
		}

		void SetColor(Color c, char op)
		{
			SetColor((int)c, op);
		}



		public void SetKeypadMode(bool application)
		{
			var code =
				application
				 ? Escape + "="
				 : Escape + ">";
			Output(code);
		}

		public void SetCursorMode(bool application)
		{
			var code =
				application
				 ? Escape + "[?1" + Code.Enable
				 : Escape + "[?1" + Code.Disable;
			Output(code);
		}

		// query state?

		public void QueryCursorPosition()
		{
			Output(Escape + "[6n");
		}

		public void TabClear()
		{
			Output(Escape + "[0g");
		}

		public void TabClearAll()
		{
			Output(Escape + "[3g");
		}

		public void TabSet()
		{
			Output(Escape + "h");
		}

		public void TabNext(byte n = 1)
		{
			WriteCode('l', n);
		}

		public void TabPrevious(byte n = 1)
		{
			WriteCode('l', n);
		}

		public void QueryAttributes()
		{
			Output(Escape + "[0c");
		}

		public void SetForeground(byte r, byte g, byte b)
		{
			SetColor(r, g, b, true);
		}

		public void SetBackground(byte r, byte g, byte b)
		{
			SetColor(r, g, b, false);
		}

		void SetColor(byte r, byte g, byte b, bool foreground = true)
		{
			byte arg0 = foreground ? (byte)38 : (byte)48;
			Span<byte> args = stackalloc byte[5] { arg0, 2, r, g, b };
			WriteCode(Code.Format, args);
		}

		public void SetMode(bool ascii)
		{
			var code =
				ascii
				? Escape + "(0"
				: Escape + "(B";
			Output(code);
		}

		void SetColor(int c, char op)
		{
			char[] code = buffer;
			int i = 0;
			code[i++] = Escape;
			code[i++] = ValueStart;
			code[i++] = op;
			code[i++] = (char)('0' + (int)c);
			code[i++] = Code.Format;
			Output(code, i);
		}

		//public void SetScrollMargin(byte t, byte b)
		//{
		//	char[] code = buffer;
		//	int i = 0;
		//	code[i++] = Escape;
		//	code[i++] = ValueStart;
		//	i = code.WriteValue(i, t);
		//	code[i++] = ValueContinue;
		//	i = code.WriteValue(i, b);
		//	code[i++] = 'r';
		//	Output(code, i);
		//}

		public void SetWindowTitle(string str)
		{
			if (str.Length > 255) throw new ArgumentException();
			char[] code = buffer;
			int i = 0;
			code[i++] = Escape;
			code[i++] = ']';
			code[i++] = '2';
			code[i++] = ';';
			foreach (var c in str)
			{
				code[i++] = c;
			}
			code[i++] = '\a';
			Output(code, i);
		}

		public void SetWide(bool wide)
		{
			var code =
				wide
				? Escape + "[?3h"
				: Escape + "[?3l";
			Output(code);
		}

		public void SoftReset()
		{
			Output(Escape + "[!p");
		}

		public void NewBuffer()
		{
			Output(Escape + "[?1049h");
		}

		public void MainBuffer()
		{
			Output(Escape + "[?1049l");
		}

		void WriteCode(char c, byte count)
		{
			if (count == 0)
			{
				return;
			}

			char[] code = buffer;
			int i = 0;
			code[i++] = Escape;
			if (count > 1)
			{
				code[i++] = ValueStart;
				i = code.WriteValue(i, count);
			}

			code[i++] = c;
			Output(code, i);
		}

		void WriteCode(char c, Span<byte> args)
		{
			char[] code = buffer;
			int i = 0;
			code[i++] = Escape;
			for (int a = 0; a < args.Length; a++)
			{
				code[i++] = a == 0 ? ValueStart : ValueContinue;
				i = code.WriteValue(i, args[a]);
			}

			code[i++] = c;
			Output(code, i);
		}


		static char GetCode(Direction d)
		{
			return (char)(Code.CursorUp + (int)d);
		}

		static char GetCode(ScrollDirection dir)
		{
			return dir == ScrollDirection.Up ? Code.ScrollUp : Code.ScrollDown;
		}

		void Output(char[] buffer, int len)
		{
			this.output.Write(buffer, 0, len);
			this.counter += len;
		}
		void Output(string str)
		{
			this.output.Write(str);
			this.counter += str.Length;
		}

		void Output(char c)
		{
			this.output.Write(c);
			this.counter++;
		}

		public void SetInvert(bool on = true)
		{
			throw new NotImplementedException();
		}
	}
}
