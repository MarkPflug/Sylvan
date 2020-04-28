using System;
using System.Runtime.InteropServices;

namespace Sylvan.Terminal
{
	partial class ColorConsole
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
		enum ConsoleMode : uint
		{
			EnableProcessedOutput = 0x0001,
			EnableWrapAtEolOutput = 0x0002,
			EnableVirtualTerminalProcessing = 0x0004,
			DisableNewLineAutoReturn = 0x0008,
		}

		static bool EnableVTProcessing()
		{
			try
			{
				var h = GetStdHandle(new IntPtr(ConsoleDevice.StdOutput));

				uint flags = 0;
				GetConsoleMode(h, out flags);
				flags |= (uint)ConsoleMode.EnableVirtualTerminalProcessing;
				var result = SetConsoleMode(h, flags);
				return result != 0;
			}
			catch (Exception)
			{
				return false;
			}
		}

		const string Kernel32 = "kernel32.dll";

		[DllImport(Kernel32)]
		static extern IntPtr GetStdHandle(IntPtr handle);

		[DllImport(Kernel32)]
		static extern int GetConsoleMode(IntPtr handle, out uint mode);

		[DllImport(Kernel32)]
		static extern int SetConsoleMode(IntPtr handle, uint mode);
	}
}
