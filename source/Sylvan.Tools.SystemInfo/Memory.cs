using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace Sylvan.Tools
{
	static class Memory
	{
		internal struct MEMORYSTATUSEX
		{
			internal uint dwLength;

			internal uint dwMemoryLoad;

			internal ulong TotalPhys;

			internal ulong AvailPhys;

			internal ulong TotalPageFile;

			internal ulong AvailPageFile;

			internal ulong TotalVirtual;

			internal ulong AvailVirtual;

			internal ulong AvailExtendedVirtual;

			internal void Init()
			{
				dwLength = checked((uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX)));
			}
		}

		[DllImport("Kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		internal static extern bool GlobalMemoryStatusEx(ref MEMORYSTATUSEX lpBuffer);

		[MethodImpl(MethodImplOptions.NoInlining)]
		static MemoryInfo GetMemoryWindows()
		{
			MEMORYSTATUSEX ex = default;
			ex.Init();
			if (GlobalMemoryStatusEx(ref ex))
			{
				return new MemoryInfo((long)ex.AvailPhys, (long)ex.TotalPhys);
			}
			return null;
		}

		static Regex r = new Regex(@"^([^:]+):\s*(\d+) kB$");

		static MemoryInfo GetMemoryLinux()
		{
			var str = File.ReadAllText("/proc/meminfo");
			var sr = new StringReader(str);
			string line;
			long total = -1;
			long avail = -1;

			while ((line = sr.ReadLine()) != null)
			{
				var s = r.Match(line);
				if (s.Groups[1].Value == "MemTotal")
				{
					total = long.Parse(s.Groups[2].Value) * 1024;
				}
				if (s.Groups[1].Value == "MemAvailable")
				{
					avail = long.Parse(s.Groups[2].Value) * 1024;
				}
				if (total > 0 && avail > 0)
					break;
			}
			if (total == -1 || avail == -1)
				return null;
			return new MemoryInfo(avail, total);
		}

		internal static MemoryInfo GetMemoryInfo()
		{
			try
			{
				if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
				{
					return GetMemoryWindows();
				}
				else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
				{
					return GetMemoryLinux();
				}
				else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
				{
					// not sure here...
					return null;
				}
			}
			catch { }
			return null;
		}
	}

	class MemoryInfo
	{
		public MemoryInfo(long avail, long total)
		{
			this.Available = avail;
			this.Total = total;
		}

		public long Available { get; }
		public long Total { get; }
	}
}
