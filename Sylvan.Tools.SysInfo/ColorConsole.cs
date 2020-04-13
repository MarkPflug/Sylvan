using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;


static class ColorConsole
{
	static class ConsoleDevice
	{
		public const uint StdInput = unchecked((uint)-10);
		public const uint StdOutput = unchecked((uint)-11);
		public const uint StdError = unchecked((uint)-12);
	}

	[Flags]
	enum ConsoleInputMode : uint
	{
		EchoInput = 0x0004,
		EnableExtendedFlags = 0x0080,
		EnableInsertMode = 0x0020,
		EnableLineInput = 0x0002,
		EnableMouseInput = 0x0010,
		EnableProcessedInput = 0x0001,
		EnableQuickEditMode = 0x0040,
		EnableWindowInput = 0x0040,
		EnableVirtualTerminalInput = 0x0200,
	}

	[Flags]
	enum ConsoleOutputMode : uint
	{
		EnableProcessedOutput = 0x0001,
		EnableWrapAtEolOutput = 0x0002,
		EnableVirtualTerminalProcessing = 0x0004,
		DisableNewLineAutoReturn = 0x0008,
	}

	// NoInlining, because I think it will avoid a potential JIT error on a non-windows platform. 
	[MethodImpl(MethodImplOptions.NoInlining)]
	public static bool EnableColorMode()
	{
		var h = GetStdHandle(new IntPtr(ConsoleDevice.StdOutput));
		var flags = ConsoleOutputMode.EnableProcessedOutput | ConsoleOutputMode.EnableVirtualTerminalProcessing;
		return SetConsoleMode(h, (uint)flags);
	}

	[DllImport("kernel32.dll")]
	static extern IntPtr GetStdHandle(IntPtr handle);

	[DllImport("kernel32.dll")]
	static extern bool SetConsoleMode(IntPtr handle, uint mode);

	public static void SetForeground(byte r, byte g, byte b)
	{
		SetColor(true, r, g, b);
	}

	public static void SetBackground(byte r, byte g, byte b)
	{
		SetColor(false, r, g, b);
	}

	const char Escape = '\x1b';

	public static void SetDefaults()
	{
		Span<char> span = stackalloc char[4];
		span[0] = Escape;
		span[1] = '[';
		span[2] = '0';
		span[3] = 'm';

		Console.Out.Write(span);
	}

	public static void SetCode(string str)
	{
		if (str == null || str.Length == 0 || str.Length > 2) return; // nope

		Span<char> span = stackalloc char[5];
		span[0] = Escape;
		span[1] = '[';

		for (int i = 0; i < str.Length; i++)
		{
			span[2 + i] = str[i];
		}
		span[2 + str.Length] = 'm';

		Console.Out.Write(span.Slice(0, 3 + str.Length));
	}

	public static void SetColor(bool foreground, byte r, byte g, byte b)
	{
		Span<char> span = stackalloc char[32]; // more than enough room
		span[0] = Escape;
		span[1] = '[';
		span[2] = foreground ? '3' : '4';
		span[3] = '8';
		span[4] = ';';
		span[5] = '2';
		span[6] = ';';

		var idx = 7;

		var sub = span.Slice(idx);
		idx += WriteByte(sub, r);
		sub = span.Slice(idx);
		sub[0] = ';';
		idx++;
		sub = span.Slice(idx);
		idx += WriteByte(sub, g);
		sub = span.Slice(idx);
		sub[0] = ';';
		idx++;
		sub = span.Slice(idx);
		idx += WriteByte(sub, b);
		sub = span.Slice(idx);
		sub[0] = 'm';
		idx++;
		Console.Out.Write(span.Slice(0, idx));
	}

	static int WriteByte(Span<char> buffer, byte value)
	{
		int len = StringLength(value);
		int index = len;

		Debug.Assert(buffer.Length >= len);

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
}
